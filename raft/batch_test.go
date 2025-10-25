package raft

import (
	"bytes"
	"encoding/gob"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
)

// Test that submitting a burst of write commands results in the final
// committed value being the last one submitted on all replicas. This test
// is intentionally tolerant of whether the implementation batches writes
// (so the number of committed log entries may be < submits) — the
// important property is that the replication is consistent and the final
// value is applied on every live replica.
func TestBatchWritesFinalValue(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)

	cs := CreateNewCluster(t, 3)
	defer cs.Shutdown()

	gob.Register(Write{})
	gob.Register(Read{})
	gob.Register(AddServers{})
	gob.Register(RemoveServers{})

	leaderId, _, _ := cs.CheckUniqueLeader()

	// Rapidly submit several writes to the leader.
	key := "batch-key"
	const N = 10
	for i := 0; i < N; i++ {
		if ok, _, _ := cs.SubmitToServer(leaderId, Write{Key: key, Val: i}); !ok {
			t.Fatalf("leader rejected SubmitToServer for %d", i)
		}
	}

	// Allow some time for replication/commit to complete.
	time.Sleep(500 * time.Millisecond)

	// Verify that each active server has the final value (N-1) for the key.
	for id := range cs.activeServers.peerSet {
		raw, found := cs.dbCluster[id].Get(key)
		if !found {
			t.Fatalf("server %d: key %q not found in db", id, key)
		}
		var v int
		dec := gob.NewDecoder(bytes.NewBuffer(raw))
		if err := dec.Decode(&v); err != nil {
			t.Fatalf("server %d: decode error: %v", id, err)
		}
		if v != N-1 {
			t.Fatalf("server %d: want final value %d, got %d", id, N-1, v)
		}
	}
}

// Smoke test: submit mixed writes and config-change commands quickly and
// ensure cluster remains functional (no panics) and all submitted writes
// are eventually applied to a majority.
func TestBatchMixedCommands(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)

	cs := CreateNewCluster(t, 3)
	defer cs.Shutdown()

	gob.Register(Write{})
	gob.Register(Read{})
	gob.Register(AddServers{})
	gob.Register(RemoveServers{})

	leaderId, _, _ := cs.CheckUniqueLeader()

	// Submit a mix: writes and a config change
	if ok, _, _ := cs.SubmitToServer(leaderId, Write{Key: "k1", Val: 1}); !ok {
		t.Fatalf("leader rejected write")
	}
	if ok, _, _ := cs.SubmitToServer(leaderId, AddServers{ServerIds: []int{3}}); !ok {
		t.Fatalf("leader rejected add-servers")
	}
	if ok, _, _ := cs.SubmitToServer(leaderId, Write{Key: "k1", Val: 2}); !ok {
		t.Fatalf("leader rejected write")
	}

	// Give time for commit and membership change to propagate.
	time.Sleep(800 * time.Millisecond)

	// The last write should be visible on a majority of nodes.
	numFound := 0
	for id := range cs.activeServers.peerSet {
		raw, ok := cs.dbCluster[id].Get("k1")
		if !ok {
			continue
		}
		var v int
		dec := gob.NewDecoder(bytes.NewBuffer(raw))
		if err := dec.Decode(&v); err != nil {
			t.Fatalf("decode error: %v", err)
		}
		if v == 2 {
			numFound++
		}
	}
	if numFound*2 <= cs.activeServers.Size() {
		t.Fatalf("expected majority to have final write, got %d/%d", numFound, cs.activeServers.Size())
	}
}

