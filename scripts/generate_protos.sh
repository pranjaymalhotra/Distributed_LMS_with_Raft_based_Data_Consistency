#!/usr/bin/env bash
# Script to generate Python code from protocol buffer definitions
set -euo pipefail

echo "Generating Python code from protocol buffers..."

# Directory housekeeping -----------------------------------------------------
SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &>/dev/null && pwd )"
PROJECT_ROOT="$( cd -- "$SCRIPT_DIR/.." &>/dev/null && pwd )"
cd "$PROJECT_ROOT"

# Output packages (also act as import paths)
for pkg in raft lms tutoring; do
  mkdir -p "$pkg"
  touch "$pkg/__init__.py"        # mark each as a real Python package
done

# gRPC code generation -------------------------------------------------------
echo "Generating Raft protocol…"
python -m grpc_tools.protoc \
  -I protos \
  --python_out=raft \
  --grpc_python_out=raft \
  protos/raft.proto

echo "Generating LMS protocol…"
python -m grpc_tools.protoc \
  -I protos \
  --python_out=lms \
  --grpc_python_out=lms \
  protos/lms.proto

echo "Generating Tutoring protocol…"
python -m grpc_tools.protoc \
  -I protos \
  --python_out=tutoring \
  --grpc_python_out=tutoring \
  protos/tutoring.proto

# No post-generation sed hacks needed! ---------------------------------------

echo ""
echo "✅ Protocol buffer generation complete!"
echo ""
echo "Generated files:"
printf "  - raft/raft_pb2*.py\n  - lms/lms_pb2*.py\n  - tutoring/tutoring_pb2*.py\n"

echo ""
echo "Tip: start components with the -m flag, e.g."
echo "     python -m tutoring.server --port 7001"
