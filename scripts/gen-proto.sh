#!/usr/bin/env bash
# Generate Go stubs from api/proto/v1/scheduler.proto.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"

export PATH="$(go env GOPATH)/bin:${PATH}"

protoc --go_out=. --go_opt=module=distr-scheduling \
  --go-grpc_out=. --go-grpc_opt=module=distr-scheduling \
  api/proto/v1/scheduler.proto
