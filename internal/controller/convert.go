package controller

import (
	"fmt"
	"sort"
	"time"

	pb "distr-scheduling/api/proto/v1"
	"distr-scheduling/internal/cluster"
	"distr-scheduling/internal/job"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func specFromProto(p *pb.JobSpec) (job.Spec, error) {
	if p == nil {
		return job.Spec{}, fmt.Errorf("spec is required")
	}
	spec := job.Spec{
		Command: p.GetCommand(),
		Args:    append([]string(nil), p.GetArgs()...),
	}
	if env := p.GetEnv(); len(env) > 0 {
		spec.Env = make(map[string]string, len(env))
		for k, v := range env {
			spec.Env[k] = v
		}
	}
	if r := p.GetResources(); r != nil {
		spec.Resources = job.ResourceRequest{
			NodesRequired: int(r.GetNodesRequired()),
			CPUsPerNode:   int(r.GetCpusPerNode()),
		}
		if r.GetMaxDuration() != nil {
			spec.Resources.MaxDuration = r.GetMaxDuration().AsDuration()
		}
	}
	if err := spec.Validate(); err != nil {
		return job.Spec{}, err
	}
	return spec, nil
}

func jobToProto(j *job.Job) *pb.Job {
	if j == nil {
		return nil
	}
	out := &pb.Job{
		Id:              j.ID,
		Status:          jobStatusToProto(j.Status),
		AssignedNodeIds: append([]string(nil), j.AssignedNodeIDs...),
		ExitCode:        int32(j.ExitCode),
		ErrorMessage:    j.ErrorMessage,
		Spec:            specToProto(j.Spec),
	}
	if ts := timestampOrNil(j.SubmittedAt); ts != nil {
		out.SubmittedAt = ts
	}
	if ts := timestampOrNil(j.StartedAt); ts != nil {
		out.StartedAt = ts
	}
	if ts := timestampOrNil(j.FinishedAt); ts != nil {
		out.FinishedAt = ts
	}
	return out
}

func specToProto(s job.Spec) *pb.JobSpec {
	p := &pb.JobSpec{
		Command: s.Command,
		Args:    append([]string(nil), s.Args...),
		Env:     s.Env,
		Resources: &pb.ResourceRequest{
			NodesRequired: int32(s.Resources.NodesRequired),
			CpusPerNode:   int32(s.Resources.CPUsPerNode),
		},
	}
	if s.Resources.MaxDuration > 0 {
		p.Resources.MaxDuration = durationpb.New(s.Resources.MaxDuration)
	}
	return p
}

func jobStatusToProto(s job.Status) pb.JobStatus {
	switch s {
	case job.StatusPending:
		return pb.JobStatus_JOB_STATUS_PENDING
	case job.StatusRunning:
		return pb.JobStatus_JOB_STATUS_RUNNING
	case job.StatusCompleted:
		return pb.JobStatus_JOB_STATUS_COMPLETED
	case job.StatusFailed:
		return pb.JobStatus_JOB_STATUS_FAILED
	case job.StatusCancelled:
		return pb.JobStatus_JOB_STATUS_CANCELLED
	default:
		return pb.JobStatus_JOB_STATUS_UNSPECIFIED
	}
}

func jobStatusFromProto(s pb.JobStatus) (job.Status, bool) {
	switch s {
	case pb.JobStatus_JOB_STATUS_PENDING:
		return job.StatusPending, true
	case pb.JobStatus_JOB_STATUS_RUNNING:
		return job.StatusRunning, true
	case pb.JobStatus_JOB_STATUS_COMPLETED:
		return job.StatusCompleted, true
	case pb.JobStatus_JOB_STATUS_FAILED:
		return job.StatusFailed, true
	case pb.JobStatus_JOB_STATUS_CANCELLED:
		return job.StatusCancelled, true
	default:
		return "", false
	}
}

func nodeToProto(n cluster.Node) *pb.Node {
	status := pb.NodeStatus_NODE_STATUS_ONLINE
	switch n.Status {
	case cluster.StatusOffline:
		status = pb.NodeStatus_NODE_STATUS_OFFLINE
	case cluster.StatusOnline:
		if n.CurrentJobID != "" {
			status = pb.NodeStatus_NODE_STATUS_BUSY
		}
	default:
		status = pb.NodeStatus_NODE_STATUS_UNSPECIFIED
	}
	out := &pb.Node{
		Id:            n.ID,
		Hostname:      n.Hostname,
		Address:       n.Address,
		Status:        status,
		TotalCpus:     int32(n.TotalCPUs),
		AllocatedCpus: int32(n.AllocatedCPUs),
		CurrentJobId:  n.CurrentJobID,
	}
	if ts := timestampOrNil(n.LastHeartbeat); ts != nil {
		out.LastHeartbeat = ts
	}
	return out
}

func timestampOrNil(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

func sortJobs(jobs []*job.Job) {
	sort.Slice(jobs, func(i, j int) bool {
		if !jobs[i].SubmittedAt.Equal(jobs[j].SubmittedAt) {
			return jobs[i].SubmittedAt.Before(jobs[j].SubmittedAt)
		}
		return jobs[i].ID < jobs[j].ID
	})
}
