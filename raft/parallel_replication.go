package raft

// appendResp represents a response from a follower's AppendEntries RPC.
// Used for concurrent response collection in parallel replication.
type appendResp struct {
	peer         uint64 // Follower peer ID
	success      bool   // Whether AppendEntries succeeded
	matchIndex   uint64 // Highest index replicated on this peer (on success)
	nextIndexHint uint64 // Suggested nextIndex (on failure, for fast backup)
	term         uint64 // Follower's term (for detecting stale leader)
}

// replicateToFollowersParallel sends AppendEntries RPCs to all followers concurrently.
// This is the core parallel replication optimization:
// - Spawns one goroutine per follower to send AppendEntries
// - Collects responses via a channel as they arrive
// - Updates matchIndex/nextIndex immediately upon each response
// - Advances commitIndex as soon as majority responds (doesn't wait for stragglers)
//
// Must be called by leader only. Expects rn.mu to be held initially but releases it
// during RPC calls (network I/O should not hold locks).
func (rn *RaftNode) replicateToFollowersParallel() {
	rn.mu.Lock()
	
	// Snapshot state needed for replication
	savedTerm := rn.currentTerm
	savedLastIndex := uint64(len(rn.log)) // 1-based indexing
	
	// Get list of followers
	followers := make([]uint64, 0)
	for peer := range rn.peerList.peerSet {
		if peer != rn.id {
			followers = append(followers, peer)
		}
	}
	
	if len(followers) == 0 {
		rn.mu.Unlock()
		return // Single-node cluster, nothing to replicate
	}
	
	rn.mu.Unlock()
	
	// Channel to collect responses from all followers
	respCh := make(chan appendResp, len(followers))
	
	// Launch concurrent goroutines to send AppendEntries to each follower
	for _, peer := range followers {
		go rn.sendAppendEntriesToPeer(peer, savedTerm, savedLastIndex, respCh)
	}
	
	// Collect responses as they arrive (concurrent response handling)
	// This is the key optimization: we process responses immediately and
	// advance commitIndex as soon as majority responds
	for i := 0; i < len(followers); i++ {
		resp := <-respCh
		rn.handleAppendEntriesResponse(resp, savedTerm)
	}
}

// sendAppendEntriesToPeer sends AppendEntries RPC to a single follower.
// Runs in its own goroutine for parallel replication.
func (rn *RaftNode) sendAppendEntriesToPeer(peer uint64, savedTerm uint64, savedLastIndex uint64, respCh chan<- appendResp) {
	rn.mu.Lock()
	
	// Check if still leader with same term
	if rn.state != Leader || rn.currentTerm != savedTerm {
		rn.mu.Unlock()
		respCh <- appendResp{peer: peer, success: false}
		return
	}
	
	// Build AppendEntries args for this peer
	nextIndex := rn.nextIndex[peer]
	prevLogIndex := int(nextIndex) - 1
	prevLogTerm := uint64(0)
	
	if prevLogIndex > 0 {
		prevLogTerm = rn.log[uint64(prevLogIndex)-1].Term
	}
	
	// Get entries to send (from nextIndex to end of log)
	entries := rn.log[int(nextIndex)-1:]
	
	args := AppendEntriesArgs{
		Term:         savedTerm,
		LeaderId:     rn.id,
		PrevLogIndex: uint64(prevLogIndex),
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rn.commitIndex,
	}
	
	rn.mu.Unlock()
	
	// Send RPC (network I/O without holding lock)
	var reply AppendEntriesReply
	ok := rn.server.RPC(peer, "RaftNode.AppendEntries", args, &reply) == nil
	
	if !ok {
		// Network failure or timeout
		respCh <- appendResp{peer: peer, success: false}
		return
	}
	
	// Build response
	resp := appendResp{
		peer:    peer,
		success: reply.Success,
		term:    reply.Term,
	}
	
	if reply.Success {
		// Calculate matchIndex: peer has replicated up to prevLogIndex + len(entries)
		resp.matchIndex = args.PrevLogIndex + uint64(len(args.Entries))
	} else {
		// Failure: suggest decrementing nextIndex (or use conflict optimization if available)
		rn.mu.Lock()
		currentNext := rn.nextIndex[peer]
		rn.mu.Unlock()
		
		if currentNext > 1 {
			resp.nextIndexHint = currentNext - 1
		} else {
			resp.nextIndexHint = 1
		}
	}
	
	respCh <- resp
}

// handleAppendEntriesResponse processes a single AppendEntries response.
// Updates matchIndex/nextIndex and advances commitIndex if majority achieved.
func (rn *RaftNode) handleAppendEntriesResponse(resp appendResp, savedTerm uint64) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	
	// Ignore stale responses
	if rn.currentTerm != savedTerm || rn.state != Leader {
		return
	}
	
	// Check if follower has higher term (we're stale leader)
	if resp.term > rn.currentTerm {
		rn.debug("Stepping down: follower %d has higher term %d", resp.peer, resp.term)
		rn.becomeFollower(resp.term)
		return
	}
	
	if resp.success {
		// Update matchIndex and nextIndex on success
		if resp.matchIndex > rn.matchIndex[resp.peer] {
			rn.matchIndex[resp.peer] = resp.matchIndex
			rn.nextIndex[resp.peer] = resp.matchIndex + 1
			rn.debug("Updated matchIndex[%d]=%d, nextIndex[%d]=%d", 
				resp.peer, resp.matchIndex, resp.peer, rn.nextIndex[resp.peer])
		}
		
		// Try to advance commitIndex (critical optimization: do this immediately)
		rn.advanceCommitIndex()
	} else {
		// Decrement nextIndex on failure and retry will happen on next heartbeat
		if resp.nextIndexHint > 0 && resp.nextIndexHint < rn.nextIndex[resp.peer] {
			rn.nextIndex[resp.peer] = resp.nextIndexHint
			rn.debug("Decremented nextIndex[%d]=%d after rejection", resp.peer, resp.nextIndexHint)
		}
	}
}

// advanceCommitIndex checks if a majority of peers have replicated an entry
// and advances commitIndex accordingly. This is called after each successful
// AppendEntries response to commit as soon as majority is achieved.
//
// Must be called with rn.mu held.
func (rn *RaftNode) advanceCommitIndex() {
	// Iterate from commitIndex+1 to lastIndex to find new committable entries
	lastIndex := uint64(len(rn.log)) // 1-based indexing
	
	for N := rn.commitIndex + 1; N <= lastIndex; N++ {
		// Count how many peers have replicated entry at index N
		replicaCount := 1 // Leader always has it
		
		for peer := range rn.peerList.peerSet {
			if peer != rn.id && rn.matchIndex[peer] >= N {
				replicaCount++
			}
		}
		
		// Check if majority has replicated this entry
		majority := (rn.peerList.Size() / 2) + 1
		if replicaCount >= majority {
			// Safety check: only commit entries from current term
			// (Raft safety rule: leader can't commit entries from previous terms directly)
			if rn.log[N-1].Term == rn.currentTerm {
				rn.debug("Advancing commitIndex from %d to %d (majority=%d/%d)", 
					rn.commitIndex, N, replicaCount, rn.peerList.Size())
				rn.commitIndex = N
				
				// Notify applier to apply newly committed entries
				rn.newCommitReady <- struct{}{}
			}
		}
	}
}

// EnableParallelReplication enables the parallel replication optimization.
// When enabled, leader sends AppendEntries to all followers concurrently
// and commits as soon as majority responds (doesn't wait for stragglers).
func (rn *RaftNode) EnableParallelReplication() {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.useParallelReplication = true
	rn.debug("Parallel replication enabled")
}

// DisableParallelReplication disables parallel replication (for testing/comparison).
func (rn *RaftNode) DisableParallelReplication() {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.useParallelReplication = false
	rn.debug("Parallel replication disabled")
}
