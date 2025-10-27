package raft

// PreVote Optimization:
// 
// Problem: When a follower gets isolated from the cluster, it starts election
// timeouts and increments its term repeatedly. When it reconnects, its higher
// term forces the current leader to step down, causing unnecessary disruption.
//
// Solution: Before starting a real election (incrementing term), the candidate
// first runs a "pre-vote" phase:
// 1. Send RequestPreVote RPCs (WITHOUT incrementing term)
// 2. If majority would vote for you, proceed to real election
// 3. If not, stay as follower (term unchanged)
//
// This prevents isolated nodes from inflating their term, as they won't get
// majority responses and won't increment their term.

// RequestPreVoteArgs - Arguments for PreVote RPC (similar to RequestVote but without term increment)
type RequestPreVoteArgs struct {
	Term         uint64 // Candidate's current term (NOT incremented yet)
	CandidateId  uint64 // Candidate requesting pre-vote
	LastLogIndex uint64 // Index of candidate's last log entry
	LastLogTerm  uint64 // Term of candidate's last log entry
}

// RequestPreVoteReply - Reply to PreVote RPC
type RequestPreVoteReply struct {
	Term        uint64 // Current term, for candidate to update itself
	VoteGranted bool   // True if follower would vote for candidate in real election
}

// RequestPreVote RPC handler - Determines if this node would vote for candidate
// WITHOUT actually granting the vote or updating term/votedFor.
//
// A node grants pre-vote if:
// 1. Candidate's term >= current term (or candidate's log is more up-to-date)
// 2. Candidate's log is at least as up-to-date as receiver's log
//
// This handler does NOT modify term or votedFor - it's a "dry run" vote check.
func (rn *RaftNode) RequestPreVote(args RequestPreVoteArgs, reply *RequestPreVoteReply) error {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	// Always tell candidate our current term
	reply.Term = rn.currentTerm
	reply.VoteGranted = false

	// If candidate's term is less than ours, reject
	if args.Term < rn.currentTerm {
		rn.debug("PreVote rejected for candidate %d: stale term (%d < %d)",
			args.CandidateId, args.Term, rn.currentTerm)
		return nil
	}

	// Check if candidate's log is at least as up-to-date as ours
	lastLogIndex, lastLogTerm := rn.lastLogIndexAndTerm()

	logIsUpToDate := args.LastLogTerm > lastLogTerm ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)

	if !logIsUpToDate {
		rn.debug("PreVote rejected for candidate %d: log not up-to-date (candidate: term=%d idx=%d, ours: term=%d idx=%d)",
			args.CandidateId, args.LastLogTerm, args.LastLogIndex, lastLogTerm, lastLogIndex)
		return nil
	}

	// Would grant vote in real election
	reply.VoteGranted = true
	rn.debug("PreVote granted to candidate %d (term=%d)", args.CandidateId, args.Term)
	return nil
}

// sendRequestPreVote - Send PreVote RPC to a peer
func (rn *RaftNode) sendRequestPreVote(peerId uint64, args *RequestPreVoteArgs, reply *RequestPreVoteReply) bool {
	err := rn.server.RPC(peerId, "RaftNode.RequestPreVote", args, reply)
	if err != nil {
		return false
	}
	return true
}

// runPreVotePhase - Run pre-vote phase before real election
//
// Returns:
// - true: Got majority pre-votes, safe to start real election
// - false: Didn't get majority, should not start election (prevents term inflation)
//
// This function does NOT modify term or votedFor - it's purely a vote check.
func (rn *RaftNode) runPreVotePhase() bool {
	rn.mu.Lock()
	savedTerm := rn.currentTerm
	lastLogIndex, lastLogTerm := rn.lastLogIndexAndTerm()

	args := RequestPreVoteArgs{
		Term:         savedTerm,
		CandidateId:  rn.id,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	rn.debug("Starting PRE-VOTE phase for term %d", savedTerm)
	rn.mu.Unlock()

	// Collect pre-votes from all peers concurrently
	type voteResult struct {
		peerId      uint64
		voteGranted bool
		term        uint64
	}

	voteChan := make(chan voteResult, rn.peerList.Size())

	// Send PreVote RPCs to all peers
	for peerId := range rn.peerList.peerSet {
		go func(peer uint64) {
			var reply RequestPreVoteReply
			ok := rn.sendRequestPreVote(peer, &args, &reply)
			if ok {
				voteChan <- voteResult{
					peerId:      peer,
					voteGranted: reply.VoteGranted,
					term:        reply.Term,
				}
			} else {
				voteChan <- voteResult{peerId: peer, voteGranted: false}
			}
		}(peerId)
	}

	// Count pre-votes (including self)
	votesReceived := 1 // Vote for self
	peerCount := rn.peerList.Size()
	
	// Collect ALL responses (important: don't return early or goroutines will block)
	for i := 0; i < peerCount; i++ {
		result := <-voteChan

		rn.mu.Lock()
		// If we see higher term, update and abort
		if result.term > savedTerm {
			rn.debug("PRE-VOTE: Discovered higher term %d from peer %d, aborting", result.term, result.peerId)
			if result.term > rn.currentTerm {
				rn.currentTerm = result.term
				rn.state = Follower
				rn.votedFor = 0
				rn.persistToStorage()
			}
			rn.mu.Unlock()
			// Continue collecting remaining responses to avoid goroutine leak
			for j := i + 1; j < peerCount; j++ {
				<-voteChan
			}
			return false
		}

		if result.voteGranted {
			votesReceived++
			rn.debug("PRE-VOTE: Received pre-vote from peer %d (%d/%d votes)",
				result.peerId, votesReceived, peerCount+1)
		}
		rn.mu.Unlock()
	}

	// Check if we got majority
	if votesReceived*2 > (peerCount + 1) {
		rn.debug("PRE-VOTE: Got majority (%d/%d), can proceed to real election",
			votesReceived, peerCount+1)
		return true
	}

	rn.debug("PRE-VOTE: Failed to get majority (%d/%d), NOT starting election",
		votesReceived, peerCount+1)
	return false
}

// EnablePreVote - Enable pre-vote optimization
func (rn *RaftNode) EnablePreVote() {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.usePreVote = true
	rn.debug("Pre-vote optimization ENABLED")
}

// DisablePreVote - Disable pre-vote optimization (for testing/comparison)
func (rn *RaftNode) DisablePreVote() {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.usePreVote = false
	rn.debug("Pre-vote optimization DISABLED")
}
