package node

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"syscall"

	pb "distr-scheduling/api/proto/v1"
)

type Result struct {
	Event    pb.JobEvent
	ExitCode int
	ErrMsg   string
}

type Executor struct {
	mu     sync.Mutex
	jobID  string
	cpus   int
	cancel context.CancelFunc
}

func NewExecutor() *Executor {
	return &Executor{}
}

func (e *Executor) Current() (jobID string, cpus int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.jobID, e.cpus
}

func (e *Executor) Stop(jobID string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.jobID == "" || (jobID != "" && e.jobID != jobID) {
		return false
	}
	if e.cancel != nil {
		e.cancel()
	}
	return true
}

func (e *Executor) Run(parent context.Context, a *pb.WorkAssignment) Result {
	if a == nil || a.GetCommand() == "" {
		return Result{Event: pb.JobEvent_JOB_EVENT_FAILED, ExitCode: 1, ErrMsg: "empty command"}
	}

	ctx := parent
	var cancel context.CancelFunc
	if d := a.GetMaxDuration().AsDuration(); d > 0 {
		ctx, cancel = context.WithTimeout(parent, d)
	} else {
		ctx, cancel = context.WithCancel(parent)
	}
	defer cancel()

	e.mu.Lock()
	if e.jobID != "" {
		e.mu.Unlock()
		return Result{Event: pb.JobEvent_JOB_EVENT_FAILED, ExitCode: 1, ErrMsg: "node busy"}
	}
	e.jobID = a.GetJobId()
	e.cpus = int(a.GetCpusPerNode())
	e.cancel = cancel
	e.mu.Unlock()

	defer e.clear(a.GetJobId())

	cmd := exec.Command(a.GetCommand(), a.GetArgs()...)
	cmd.Env = mergeEnv(os.Environ(), a.GetEnv())
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := cmd.Start(); err != nil {
		return Result{Event: pb.JobEvent_JOB_EVENT_FAILED, ExitCode: 1, ErrMsg: err.Error()}
	}

	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()

	select {
	case err := <-done:
		return resultFromWait(err)
	case <-ctx.Done():
		if cmd.Process != nil {
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		}
		<-done
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return Result{Event: pb.JobEvent_JOB_EVENT_FAILED, ExitCode: 1, ErrMsg: "max duration exceeded"}
		}
		return Result{Event: pb.JobEvent_JOB_EVENT_CANCELLED, ExitCode: 1, ErrMsg: "cancelled"}
	}
}

func (e *Executor) clear(jobID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.jobID == jobID {
		e.jobID = ""
		e.cpus = 0
		e.cancel = nil
	}
}

func resultFromWait(err error) Result {
	if err == nil {
		return Result{Event: pb.JobEvent_JOB_EVENT_COMPLETED, ExitCode: 0}
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return Result{
			Event:    pb.JobEvent_JOB_EVENT_FAILED,
			ExitCode: exitErr.ExitCode(),
			ErrMsg:   err.Error(),
		}
	}
	return Result{Event: pb.JobEvent_JOB_EVENT_FAILED, ExitCode: 1, ErrMsg: err.Error()}
}

func mergeEnv(base []string, extra map[string]string) []string {
	if len(extra) == 0 {
		return base
	}
	seen := make(map[string]int, len(base)+len(extra))
	out := append([]string(nil), base...)
	for i, kv := range out {
		for j := 0; j < len(kv); j++ {
			if kv[j] == '=' {
				seen[kv[:j]] = i
				break
			}
		}
	}
	for k, v := range extra {
		entry := fmt.Sprintf("%s=%s", k, v)
		if i, ok := seen[k]; ok {
			out[i] = entry
			continue
		}
		seen[k] = len(out)
		out = append(out, entry)
	}
	return out
}
