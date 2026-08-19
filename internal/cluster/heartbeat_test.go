package cluster

import (
	"context"
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
	failed   []string
	assigned map[string][]string
	cpus     int
}

func (f *fakeJobs) FailRunningJob(jobID, reason string) ([]string, int, bool) {
	f.failed = append(f.failed, jobID)
	return append([]string(nil), f.assigned[jobID]...), f.cpus, true
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
	r := Make()
	r.RegisterNode(Node{ID: "n1", TotalCPUs: 2})

	timeout := 40 * time.Millisecond
	m := MakeMonitor(r, timeout, 10*time.Millisecond, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go m.Run(ctx)

	m.Record("n1")
	deadline := time.Now().Add(200 * time.Millisecond)
	for {
		n, _ := r.Get("n1")
		if n.Status == StatusOnline && !n.LastHeartbeat.IsZero() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("heartbeat was not applied")
		}
		time.Sleep(2 * time.Millisecond)
	}

	deadline = time.Now().Add(300 * time.Millisecond)
	for {
		n, _ := r.Get("n1")
		if n.Status == StatusOffline {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("node was not reaped after missed heartbeats")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
