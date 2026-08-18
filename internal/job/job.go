// Job model and status definitions.
//
// A Job is a unit of work submitted by a user via slctl submit. Spec is the
// payload that must be provided at submit time; remaining fields are filled
// by the controller as the job moves through the queue.

package job

import (
	"fmt"
	"strings"
	"time"
)

type Status string

const (
	StatusPending   Status = "pending"
	StatusRunning   Status = "running"
	StatusCompleted Status = "completed"
	StatusFailed    Status = "failed"
	StatusCancelled Status = "cancelled"
)

func (s Status) Valid() bool {
	switch s {
	case StatusPending, StatusRunning, StatusCompleted, StatusFailed, StatusCancelled:
		return true
	default:
		return false
	}
}

func (s Status) Terminal() bool {
	return s == StatusCompleted || s == StatusFailed || s == StatusCancelled
}

// ResourceRequest is the gang-scheduling constraint: N nodes × M CPUs,
// optionally bounded by a wall-clock limit. The scheduler never partially
// allocates — all nodes_required nodes must be free at once.
type ResourceRequest struct {
	NodesRequired int
	CPUsPerNode   int
	MaxDuration   time.Duration
}

func (r ResourceRequest) Validate() error {
	if r.NodesRequired < 1 {
		return fmt.Errorf("nodes_required must be >= 1, got %d", r.NodesRequired)
	}
	if r.CPUsPerNode < 1 {
		return fmt.Errorf("cpus_per_node must be >= 1, got %d", r.CPUsPerNode)
	}
	if r.MaxDuration < 0 {
		return fmt.Errorf("max_duration must be >= 0, got %s", r.MaxDuration)
	}
	return nil
}

// Spec is what a client must submit. Command is argv[0]; Args are argv[1:].
type Spec struct {
	Resources ResourceRequest
	Command   string
	Args      []string
	Env       map[string]string
}

func (s Spec) Validate() error {
	if err := s.Resources.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(s.Command) == "" {
		return fmt.Errorf("command must be non-empty")
	}
	return nil
}

type Job struct {
	ID              string
	Status          Status
	Spec            Spec
	AssignedNodeIDs []string
	SubmittedAt     time.Time
	StartedAt       time.Time
	FinishedAt      time.Time
	ExitCode        int
	ErrorMessage    string
}

func New(id string, spec Spec) (*Job, error) {
	if strings.TrimSpace(id) == "" {
		return nil, fmt.Errorf("job id must be non-empty")
	}
	if err := spec.Validate(); err != nil {
		return nil, err
	}
	return &Job{
		ID:          id,
		Status:      StatusPending,
		Spec:        spec,
		SubmittedAt: time.Now().UTC(),
	}, nil
}

func (j *Job) CanTransition(to Status) bool {
	if !to.Valid() {
		return false
	}
	switch j.Status {
	case StatusPending:
		return to == StatusRunning || to == StatusCancelled
	case StatusRunning:
		return to == StatusCompleted || to == StatusFailed || to == StatusCancelled
	default:
		return false
	}
}

func (j *Job) Transition(to Status) error {
	if !j.CanTransition(to) {
		return fmt.Errorf("invalid job %s transition %s → %s", j.ID, j.Status, to)
	}
	j.Status = to
	if to.Terminal() {
		j.FinishedAt = time.Now().UTC()
	}
	return nil
}

func (j *Job) MarkRunning(nodeIDs []string) error {
	if err := j.Transition(StatusRunning); err != nil {
		return err
	}
	j.AssignedNodeIDs = append([]string(nil), nodeIDs...)
	j.StartedAt = time.Now().UTC()
	return nil
}

func (j *Job) MarkCompleted(exitCode int) error {
	if err := j.Transition(StatusCompleted); err != nil {
		return err
	}
	j.ExitCode = exitCode
	return nil
}

func (j *Job) MarkFailed(exitCode int, errMsg string) error {
	if err := j.Transition(StatusFailed); err != nil {
		return err
	}
	j.ExitCode = exitCode
	j.ErrorMessage = errMsg
	return nil
}

func (j *Job) MarkCancelled() error {
	return j.Transition(StatusCancelled)
}
