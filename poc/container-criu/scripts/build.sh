#!/bin/bash
# Build the AgentENV node server (with the container backend) from the repo root.
# Toolchain used for the POC: Ubuntu 24.04 apt rustc/cargo 1.91 (PATH=/usr/lib/rust-1.91/bin), protoc from apt.
set -uo pipefail
ROOT=$(cd "$(dirname "$0")/../../.." && pwd); cd "$ROOT"
export PROTOC=${PROTOC:-/usr/bin/protoc} PROTOC_INCLUDE=${PROTOC_INCLUDE:-/usr/include} PATH=/usr/lib/rust-1.91/bin:$PATH
LOG=${LOG:-/tmp/aenv-cargo.log}
OPENSSL_STATIC=1 cargo build --release --locked -p agentenv --bin server --features vendored-openssl > $LOG 2>&1; rc=$?
grep -E "^(error|warning)" -A6 $LOG | head -150; tail -2 $LOG
[ $rc = 0 ] && sha256sum target/release/server
exit $rc
