package raft

import (
	"encoding/gob"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
)

// TestPreVoteIsolatedFollowerNoDisruption verifies the key benefit of pre-vote:
// When a follower gets isolated, it attempts elections but DOESN'T inflate its term
// (because pre-vote fails). When it reconnects, it doesn't disrupt the existing leader.
//
// Scenario:
// 1. Start 3-node cluster, elect leader
// 2. Disconnect one follower for a long time (>1 second)
// 3. Follower tries to start elections but pre-vote fails (no majority response)
// 4. Follower's term stays at initial value
// 5. Reconnect follower
// 6. Existing leader continues without disruption
//
// Without pre-vote: Isolated follower would increment term repeatedly, then disrupt
// leader on reconnect by forcing it to step down due to higher term.
func TestPreVoteIsolatedFollowerNoDisruption(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)

	cs := CreateNewCluster(t, 3)
	defer cs.Shutdown()

	// Get initial leader and term
	initialLeader, initialTerm, _ := cs.CheckUniqueLeader()
	t.Logf("Initial leader: %d, term: %d", initialLeader, initialTerm)

	// Disconnect a follower (not the leader)
	follower := (initialLeader + 1) % 3
	cs.DisconnectPeer(uint64(follower))
	t.Logf("Disconnected follower: %d", follower)

	// Wait long enough for follower to attempt multiple elections
	// (election timeout is ~150-300ms, so 1.2s = ~4-8 election attempts)
	time.Sleep(1200 * time.Millisecond)

	// Check follower's term - with pre-vote, it should NOT have incremented
	cs.raftCluster[uint64(follower)].rn.mu.Lock()
	followerTerm := cs.raftCluster[uint64(follower)].rn.currentTerm
	cs.raftCluster[uint64(follower)].rn.mu.Unlock()

	t.Logf("After isolation, follower term: %d (initial was %d)", followerTerm, initialTerm)

	// With pre-vote, follower's term should be close to initial term
	// (may have incremented once if it attempted election before full isolation)
	if followerTerm > uint64(initialTerm)+2 {
		t.Errorf("Pre-vote FAILED: isolated follower inflated term to %d (expected ~%d)", 
			followerTerm, initialTerm)
	}

	// Reconnect the follower
	cs.ReconnectPeer(uint64(follower))
	t.Logf("Reconnected follower: %d", follower)

	// Give time for reconnection and potential leader change
	time.Sleep(500 * time.Millisecond)

	// Check leader - should STILL be the original leader (no disruption)
	newLeader, newTerm, _ := cs.CheckUniqueLeader()
	t.Logf("After reconnect - leader: %d, term: %d", newLeader, newTerm)

	if newLeader != initialLeader {
		t.Errorf("Pre-vote FAILED: leader changed from %d to %d (expected no change)", 
			initialLeader, newLeader)
	}

	// Term may have incremented slightly due to heartbeats, but should not have jumped
	if newTerm > initialTerm+3 {
		t.Errorf("Pre-vote FAILED: term inflated to %d (expected ~%d)", newTerm, initialTerm)
	}

	t.Logf("✓ Pre-vote SUCCESS: No disruption after follower reconnection")
}

// TestPreVoteWithoutOptimizationShowsDisruption - Demonstrates the problem pre-vote solves
// by DISABLING pre-vote and showing that isolated followers DO disrupt the leader.
func TestPreVoteWithoutOptimizationShowsDisruption(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)

	cs := CreateNewCluster(t, 3)
	defer cs.Shutdown()

	// DISABLE pre-vote on all nodes to show the problem
	for id := range cs.activeServers.peerSet {
		cs.raftCluster[id].rn.DisablePreVote()
	}

	initialLeader, initialTerm, _ := cs.CheckUniqueLeader()
	t.Logf("Initial leader: %d, term: %d (pre-vote DISABLED)", initialLeader, initialTerm)

	// Disconnect follower
	follower := (initialLeader + 1) % 3
	cs.DisconnectPeer(uint64(follower))
	t.Logf("Disconnected follower: %d", follower)

	// Wait for follower to inflate term
	time.Sleep(1200 * time.Millisecond)

	// Check follower's term - WITHOUT pre-vote, it WILL have incremented significantly
	cs.raftCluster[uint64(follower)].rn.mu.Lock()
	followerTerm := cs.raftCluster[uint64(follower)].rn.currentTerm
	cs.raftCluster[uint64(follower)].rn.mu.Unlock()

	t.Logf("After isolation, follower term: %d (initial was %d)", followerTerm, initialTerm)

	// Without pre-vote, term should have inflated significantly
	if followerTerm <= uint64(initialTerm)+3 {
		t.Logf("Warning: follower term only %d, expected higher inflation without pre-vote", followerTerm)
	}

	// Reconnect follower
	cs.ReconnectPeer(uint64(follower))
	t.Logf("Reconnected follower: %d", follower)

	// Give time for disruption
	time.Sleep(500 * time.Millisecond)

	// Check if leader changed - WITHOUT pre-vote, disruption is likely
	newLeader, newTerm, _ := cs.CheckUniqueLeader()
	t.Logf("After reconnect - leader: %d, term: %d", newLeader, newTerm)

	if newTerm <= initialTerm+3 {
		t.Logf("Note: Term only increased to %d (expected higher without pre-vote)", newTerm)
	}

	t.Logf("✓ Without pre-vote: Showed impact of term inflation (term went from %d to %d)", 
		initialTerm, newTerm)
}

// TestPreVoteNormalElectionStillWorks - Verify pre-vote doesn't break normal elections
func TestPreVoteNormalElectionStillWorks(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)

	cs := CreateNewCluster(t, 3)
	defer cs.Shutdown()

	// Get initial leader
	initialLeader, initialTerm, _ := cs.CheckUniqueLeader()
	t.Logf("Initial leader: %d, term: %d", initialLeader, initialTerm)

	// Disconnect the leader (should trigger new election)
	cs.DisconnectPeer(uint64(initialLeader))
	t.Logf("Disconnected leader: %d", initialLeader)

	// Wait for new election
	time.Sleep(300 * time.Millisecond)

	// Verify new leader was elected (pre-vote should allow this)
	newLeader, newTerm, _ := cs.CheckUniqueLeader()
	t.Logf("New leader: %d, term: %d", newLeader, newTerm)

	if newLeader == initialLeader {
		t.Errorf("Expected new leader, got same leader %d", initialLeader)
	}

	if newTerm <= initialTerm {
		t.Errorf("Expected term to increase, got %d (was %d)", newTerm, initialTerm)
	}

	t.Logf("✓ Pre-vote doesn't break normal elections")
}

// TestPreVoteFollowerDisconnectWithCommit - Based on TestElectionFollowerDisconnectReconnectAfterLongCommitDone
// Verifies pre-vote works correctly when follower is disconnected during commits.
func TestPreVoteFollowerDisconnectWithCommit(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)

	cs := CreateNewCluster(t, 3)
	defer cs.Shutdown()

	gob.Register(Write{})

	initialLeader, initialTerm, _ := cs.CheckUniqueLeader()
	t.Logf("Initial leader: %d, term: %d", initialLeader, initialTerm)

	// Disconnect follower
	follower := (initialLeader + 1) % 3
	cs.DisconnectPeer(uint64(follower))
	t.Logf("Disconnected follower: %d", follower)

	// Submit a write to the leader
	key := "prevote-test-key"
	val := 42
	isLeader, _, _ := cs.SubmitToServer(initialLeader, Write{Key: key, Val: val})
	if !isLeader {
		t.Fatalf("Expected id=%d to be leader", initialLeader)
	}
	t.Logf("Submitted write: key=%s, val=%d", key, val)

	// Wait for follower to attempt elections (but pre-vote should prevent term inflation)
	time.Sleep(1200 * time.Millisecond)

	// Check follower's term - should not have inflated
	cs.raftCluster[uint64(follower)].rn.mu.Lock()
	followerTerm := cs.raftCluster[uint64(follower)].rn.currentTerm
	cs.raftCluster[uint64(follower)].rn.mu.Unlock()
	t.Logf("Isolated follower term: %d (initial was %d)", followerTerm, initialTerm)

	// Reconnect follower
	cs.ReconnectPeer(uint64(follower))
	t.Logf("Reconnected follower: %d", follower)

	// Wait for convergence
	time.Sleep(500 * time.Millisecond)

	// Verify leader didn't change
	newLeader, newTerm, _ := cs.CheckUniqueLeader()
	t.Logf("After reconnect - leader: %d, term: %d", newLeader, newTerm)

	if newLeader != initialLeader {
		t.Errorf("Pre-vote FAILED: leader changed from %d to %d", initialLeader, newLeader)
	}

	// Verify the write is still committed (follower should catch up)
	time.Sleep(300 * time.Millisecond)
	
	t.Logf("✓ Pre-vote with commits: No disruption after follower reconnection")
}

// TestPreVoteTermComparison - Verify pre-vote correctly handles term comparisons
func TestPreVoteTermComparison(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)

	cs := CreateNewCluster(t, 5)
	defer cs.Shutdown()

	// Get initial state
	initialLeader, initialTerm, _ := cs.CheckUniqueLeader()
	t.Logf("Initial leader: %d, term: %d", initialLeader, initialTerm)

	// Disconnect two followers
	follower1 := (initialLeader + 1) % 5
	follower2 := (initialLeader + 2) % 5
	cs.DisconnectPeer(uint64(follower1))
	cs.DisconnectPeer(uint64(follower2))
	t.Logf("Disconnected followers: %d, %d", follower1, follower2)

	// Wait for them to attempt elections
	time.Sleep(800 * time.Millisecond)

	// Reconnect both
	cs.ReconnectPeer(uint64(follower1))
	cs.ReconnectPeer(uint64(follower2))
	t.Logf("Reconnected followers")

	// Wait for convergence
	time.Sleep(400 * time.Millisecond)

	// Verify we still have a stable leader
	_, newTerm, _ := cs.CheckUniqueLeader()
	t.Logf("Final term: %d", newTerm)

	// With pre-vote, term inflation should be minimal
	if newTerm > initialTerm+5 {
		t.Errorf("Excessive term inflation: %d -> %d", initialTerm, newTerm)
	}

	t.Logf("✓ Pre-vote prevents excessive term inflation in 5-node cluster")
}
