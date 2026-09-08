#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -euo pipefail

# Run on an ephemeral Linux CI VM. The service and mount are loopback-only; this does not
# demonstrate Kerberos, cross-host performance, provider failover, or production retention policy.
: "${EVIDENCE_DIR:?}"
: "${CANDIDATE_SHA:?}"
mkdir -p "${EVIDENCE_DIR}"
work="$(mktemp -d /tmp/spark-shared-shuffle.XXXXXX)"
export_root="${work}/export"
mount_root="${work}/mount"
local_root="${work}/attempt-local"
export_file="/etc/exports.d/spark-shuffle-poc.exports"
mkdir -p "${export_root}" "${mount_root}" "${local_root}"
chmod 700 "${export_root}"
cleanup() {
  local status=$?
  trap - EXIT
  sudo umount "${mount_root}" || true
  sudo rm -f "${export_file}"
  sudo exportfs -ra || true
  rm -rf "${work}"
  exit "${status}"
}
trap cleanup EXIT

sudo apt-get update -qq
sudo apt-get install -y --no-install-recommends nfs-kernel-server nfs-common
sudo mkdir -p /etc/exports.d
printf '%s 127.0.0.1(rw,sync,no_subtree_check,root_squash)\n' "${export_root}" | \
  sudo tee "${export_file}" >/dev/null
sudo systemctl start nfs-kernel-server
sudo exportfs -ra
sudo mount -t nfs -o vers=4,proto=tcp 127.0.0.1:"${export_root}" "${mount_root}"
findmnt --target "${mount_root}" --output SOURCE,FSTYPE,OPTIONS > "${EVIDENCE_DIR}/mount.txt"
stat -f -c '%T' "${mount_root}" | grep -Fx nfs

export SPARK_SHUFFLE_RECOVERY_TEST_MASTER='local-cluster[2,1,1024]'
export SPARK_SHUFFLE_RECOVERY_TEST_LOCAL_ROOT="${local_root}"
proof_root="${mount_root}/proof"
mkdir -p "${proof_root}"
common="root=${proof_root} scenario=sparse group=shared-filesystem testedCommit=${CANDIDATE_SHA}"
entry='org.apache.spark.shuffle.ShuffleRecoveryColdProcessProcess'
run_child() {
  local mode="$1"
  shift
  ./build/sbt -Phadoop-3 -Phive 'project sql' \
    'set Test / run / fork := true' \
    'set Test / javaOptions += "-Dspark.shuffle.useOldFetchProtocol=true"' \
    "Test/runMain ${entry} ${mode} ${common} $*" 2>&1 | \
    tee "${EVIDENCE_DIR}/${mode}.log"
}
run_child baseline "evidence=${EVIDENCE_DIR}/baseline.tsv"
run_child producer "evidence=${EVIDENCE_DIR}/producer.tsv"
# Only the mounted artifact namespace survives between producer and replacement.
rm -rf "${local_root}"
mkdir -p "${local_root}"
run_child replacement "evidence=${EVIDENCE_DIR}/replacement.tsv" \
  "baseline=${EVIDENCE_DIR}/baseline.tsv producer=${EVIDENCE_DIR}/producer.tsv"

python3 - "${EVIDENCE_DIR}" "${CANDIDATE_SHA}" <<'CHECK'
import csv
import sys
from pathlib import Path
root = Path(sys.argv[1])
def record(name):
    with (root / (name + '.tsv')).open() as stream:
        rows = list(csv.DictReader(stream, delimiter='\t'))
    assert len(rows) == 1, name
    return rows[0]
base, producer, replacement = [record(n) for n in ['baseline', 'producer', 'replacement']]
assert base['resultDigest'] == producer['resultDigest'] == replacement['resultDigest']
assert replacement['adopted'] == 'true'
assert replacement['mapTaskCount'] == '0'
assert int(replacement['providerBytesRead']) > 0
assert replacement['currentShuffleId'] != producer['originShuffleId']
(root / 'decision.txt').write_text(
    'SHARED_FILESYSTEM_MECHANISM_PASS\n'
    'Not a production provider, security, scale, AQE or value gate.\n')
CHECK
