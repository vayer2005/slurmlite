package cluster

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestTouchUnregistered(t *testing.T) {
	r := Make()
	if r.Touch("n1") {
		t.Fatal("expected Touch on unknown node to fail")
	}
}

func TestTouchMarksOnlineAndStampsClock(t *testing.T) {
	r := Make()
	r.RegisterNode(Node{ID: "n1", Status: StatusOffline, TotalCPUs: 4})

	before := time.Now()
	if !r.Touch("n1") {
		t.Fatal("expected Touch to succeed")
	}
	n, ok := r.Get("n1")
	if !ok {
		t.Fatal("node missing")
	}
	if n.Status != StatusOnline {
		t.Fatalf("status = %s, want online", n.Status)
	}
	if n.LastHeartbeat.Before(before) {
		t.Fatal("LastHeartbeat was not stamped on the controller")
	}
}

func TestReapStaleMarksOffline(t *testing.T) {
	r := Make()
	r.RegisterNode(Node{ID: "alive", TotalCPUs: 2, LastHeartbeat: time.Now()})
	r.RegisterNode(Node{ID: "dead", TotalCPUs: 2, LastHeartbeat: time.Now().Add(-time.Minute)})

	dead := r.ReapStale(time.Now(), 5*time.Second)
	if len(dead) != 1 || dead[0].ID != "dead" {
		t.Fatalf("dead = %+v, want [dead]", dead)
	}

	alive, _ := r.Get("alive")
	if alive.Status != StatusOnline {
		t.Fatalf("alive status = %s", alive.Status)
	}
	gone, _ := r.Get("dead")
	if gone.Status != StatusOffline {
		t.Fatalf("dead status = %s", gone.Status)
	}
}

type fakeJobs struct {
	mu       sync.Mutex
	failed   []string
	assigned map[string][]string
	cpus     int
}

func (f *fakeJobs) FailRunningJob(jobID, reason string) ([]string, int, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failed = append(f.failed, jobID)
	return append([]string(nil), f.assigned[jobID]...), f.cpus, true
}

func (f *fakeJobs) failedIDs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.failed...)
}

func TestHandleDeadNodesFailsJobAndReleasesGang(t *testing.T) {
	r := Make()
	r.RegisterNode(Node{ID: "n1", TotalCPUs: 4, AllocatedCPUs: 2, CurrentJobID: "j1", LastHeartbeat: time.Now().Add(-time.Minute)})
	r.RegisterNode(Node{ID: "n2", TotalCPUs: 4, AllocatedCPUs: 2, CurrentJobID: "j1", LastHeartbeat: time.Now().Add(-time.Minute)})
	r.RegisterNode(Node{ID: "n3", TotalCPUs: 4, LastHeartbeat: time.Now()})

	jobs := &fakeJobs{
		assigned: map[string][]string{"j1": {"n1", "n2"}},
		cpus:     2,
	}
	dead := r.ReapStale(time.Now(), time.Second)
	handleDeadNodes(r, jobs, dead)

	if len(jobs.failed) != 1 || jobs.failed[0] != "j1" {
		t.Fatalf("failed jobs = %v, want [j1]", jobs.failed)
	}
	for _, id := range []string{"n1", "n2"} {
		n, _ := r.Get(id)
		if n.CurrentJobID != "" || n.AllocatedCPUs != 0 {
			t.Fatalf("%s still reserved: %+v", id, n)
		}
		if n.Status != StatusOffline {
			t.Fatalf("%s status = %s, want offline", id, n.Status)
		}
	}
	n3, _ := r.Get("n3")
	if n3.Status != StatusOnline {
		t.Fatalf("n3 status = %s", n3.Status)
	}
}

func TestMonitorHeartbeatThenTimeout(t *testing.T) {
	const (
		runFor     = 10 * time.Second
		timeout    = time.Second
		reapEvery  = 200 * time.Millisecond
		beatEvery  = 200 * time.Millisecond
		crashAt    = 2 * time.Second
		jobDieAt   = 4 * time.Second
		recoverOff = 2500 * time.Millisecond
		recoverOn  = 7 * time.Second
	)

	r := Make()
	for _, id := range []string{"n-healthy", "n-crash", "n-job", "n-peer", "n-recover"} {
		r.RegisterNode(Node{ID: id, TotalCPUs: 4})
	}
	if err := r.ReserveNodes("j1", []string{"n-job", "n-peer"}, 2); err != nil {
		t.Fatal(err)
	}

	jobs := &fakeJobs{
		assigned: map[string][]string{"j1": {"n-job", "n-peer"}},
		cpus:     2,
	}
	m := MakeMonitor(r, timeout, reapEvery, jobs)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go m.Run(ctx)

	start := time.Now()
	inject, injectCancel := context.WithTimeout(context.Background(), runFor)
	defer injectCancel()

	go func() {
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		for {
			select {
			case <-inject.Done():
				return
			case <-tick.C:
				t.Logf("t=%.0fs %s", time.Since(start).Seconds(), formatNodes(r,
					"n-healthy", "n-crash", "n-job", "n-peer", "n-recover"))
			}
		}
	}()

	beat := time.NewTicker(beatEvery)
	defer beat.Stop()
loop:
	for {
		select {
		case <-inject.Done():
			break loop
		case <-beat.C:
			elapsed := time.Since(start)
			m.Record("n-healthy")
			m.Record("n-peer")
			if elapsed < crashAt {
				m.Record("n-crash")
			}
			if elapsed < jobDieAt {
				m.Record("n-job")
			}
			if elapsed < recoverOff || elapsed >= recoverOn {
				m.Record("n-recover")
			}
			m.Record("")
			m.Record("unknown")
		}
	}

	time.Sleep(reapEvery)

	assertStatus(t, r, "n-healthy", StatusOnline)
	assertStatus(t, r, "n-crash", StatusOffline)
	assertStatus(t, r, "n-job", StatusOffline)
	assertStatus(t, r, "n-peer", StatusOnline)
	assertStatus(t, r, "n-recover", StatusOnline)

	if _, ok := r.Get("unknown"); ok {
		t.Fatal("unregistered heartbeat should not create a node")
	}

	failed := jobs.failedIDs()
	if len(failed) != 1 || failed[0] != "j1" {
		t.Fatalf("failed jobs = %v, want [j1]", failed)
	}
	for _, id := range []string{"n-job", "n-peer"} {
		n, _ := r.Get(id)
		if n.CurrentJobID != "" || n.AllocatedCPUs != 0 {
			t.Fatalf("%s still reserved after node loss: %+v", id, n)
		}
	}
}

func assertStatus(t *testing.T, r *Registry, id string, want Status) {
	t.Helper()
	n, ok := r.Get(id)
	if !ok {
		t.Fatalf("node %s missing", id)
	}
	if n.Status != want {
		t.Fatalf("%s status = %s, want %s", id, n.Status, want)
	}
}

func formatNodes(r *Registry, ids ...string) string {
	parts := make([]string, 0, len(ids))
	for _, id := range ids {
		n, ok := r.Get(id)
		if !ok {
			parts = append(parts, id+"=missing")
			continue
		}
		parts = append(parts, fmt.Sprintf("%s=%s", id, n.Status))
	}
	return strings.Join(parts, " ")
}
