#!/bin/bash
# CRIU from its official source release (not in Canonical apt for noble): GitHub tag v4.3 archive, verified by
# (a) the tar's embedded commit id (`git get-tar-commit-id`) == the tag's commit 2b7ac603…, and (b) the sha256 pinned below
# (computed from the first verified download). Build deps from Canonical apt only. Installs /usr/local/sbin/criu.
set -euo pipefail
B4=${B4:-$HOME/criu-build}; V=4.3; TAG_COMMIT=2b7ac603cda31d015d91c856c34c557cb2300e7d
SHA=${CRIU_SHA256:-155d8853eec901a8a93cd6aaa5db1fa4a2106dc65ea4314e525fdaff322e52e8}
mkdir -p $B4/dl && cd $B4/dl
export DEBIAN_FRONTEND=noninteractive
apt-get update -q >/dev/null 2>&1 || true
apt-get install -y -q --no-install-recommends build-essential pkg-config git python3-protobuf protobuf-compiler protobuf-c-compiler \
  libprotobuf-dev libprotobuf-c-dev libbsd-dev libnftables-dev libcap-dev libnl-3-dev libnet1-dev libaio-dev libgnutls28-dev \
  uuid-dev iproute2 asciidoc-base xmlto > /tmp/criu-apt.log 2>&1 || { tail -5 /tmp/criu-apt.log; exit 1; }
[ -f criu-v$V.tar.gz ] || curl -fsSL -o criu-v$V.tar.gz https://github.com/checkpoint-restore/criu/archive/refs/tags/v$V.tar.gz
got=$(sha256sum criu-v$V.tar.gz | cut -d' ' -f1); echo "CRIU_TARBALL_SHA256 $got"
[ -z "$SHA" ] || [ "$got" = "$SHA" ] || { echo SHA_MISMATCH; exit 1; }
cid=$( (gzip -dc criu-v$V.tar.gz 2>/dev/null || true) | git get-tar-commit-id || true);   # gzip gets SIGPIPE once git has the header
 echo "TAR_COMMIT $cid"; [ "$cid" = "$TAG_COMMIT" ] || { echo COMMIT_MISMATCH; exit 1; }
rm -rf criu-$V && tar xzf criu-v$V.tar.gz && cd criu-$V
env -u ARCH PATH=/usr/bin:$PATH make -j32 criu > /tmp/criu-build.log 2>&1 || { tail -20 /tmp/criu-build.log; exit 1; }
install -m 0755 criu/criu /usr/local/sbin/criu
criu --version; sha256sum /usr/local/sbin/criu
criu check 2>&1 | tail -3; criu check --feature tcp_half_closed 2>&1 | tail -1
