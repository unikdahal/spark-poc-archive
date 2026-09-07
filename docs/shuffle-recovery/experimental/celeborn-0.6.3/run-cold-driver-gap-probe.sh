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

CELEBORN_VERSION="0.6.3"
CELEBORN_TAG_COMMIT="f583b73d292afdfeed865e20610838121c9db9cf"
MASTER_PORT="${CELEBORN_PROBE_MASTER_PORT:-19097}"
APP_ID="${CELEBORN_PROBE_APP_ID:-shuffle-recovery-cold-provider-probe}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK="${CELEBORN_PROBE_WORK_DIR:-$(mktemp -d)}"
RESULT="${CELEBORN_PROBE_RESULT:-${WORK}/result.txt}"
ARCHIVE="apache-celeborn-${CELEBORN_VERSION}-bin.tgz"
ARCHIVE_BASE="https://archive.apache.org/dist/celeborn/celeborn-${CELEBORN_VERSION}"
CELEBORN_HOME="${WORK}/apache-celeborn-${CELEBORN_VERSION}-bin"
STORAGE="${WORK}/worker-storage"
STATE="${WORK}/producer.properties"
PRODUCER_LOG="${WORK}/producer.log"
READER_LOG="${WORK}/reader.log"

mkdir -p "${WORK}" "${STORAGE}"
: > "${RESULT}"

log() {
  printf '%s\n' "$*" | tee -a "${RESULT}"
}

fail() {
  log "PROBE_FAILURE: $*"
  exit 1
}

file_count() {
  find "${STORAGE}" -type f 2>/dev/null | wc -l | tr -d ' '
}

file_bytes() {
  find "${STORAGE}" -type f -printf '%s\n' 2>/dev/null | awk '{n += $1} END {print n + 0}'
}

wait_for_log() {
  local file="$1"
  local pattern="$2"
  local attempts="$3"
  local i
  for ((i = 0; i < attempts; i++)); do
    if [[ -f "${file}" ]] && grep -q "${pattern}" "${file}"; then
      return 0
    fi
    sleep 1
  done
  return 1
}

cleanup() {
  set +e
  if [[ -n "${producer_pid:-}" ]] && kill -0 "${producer_pid}" 2>/dev/null; then
    kill -9 "${producer_pid}" 2>/dev/null
  fi
  if [[ -d "${CELEBORN_HOME}" ]]; then
    CELEBORN_HOME="${CELEBORN_HOME}" "${CELEBORN_HOME}/sbin/stop-worker.sh" >/dev/null 2>&1
    CELEBORN_HOME="${CELEBORN_HOME}" "${CELEBORN_HOME}/sbin/stop-master.sh" >/dev/null 2>&1
  fi
}
trap cleanup EXIT

log "provider=Apache Celeborn"
log "version=${CELEBORN_VERSION}"
log "tagCommit=${CELEBORN_TAG_COMMIT}"
log "topology=master + worker + producer JVM/LifecycleManager + independent reader JVM/new LifecycleManager"
log "appId=${APP_ID}"
log "shuffleId=1 partitionId=0"
log "masterEndpoint=127.0.0.1:${MASTER_PORT}"
log "workerStorage=${STORAGE}"

if [[ ! -f "${WORK}/${ARCHIVE}" ]]; then
  curl --fail --location --retry 3 --output "${WORK}/${ARCHIVE}" \
    "${ARCHIVE_BASE}/${ARCHIVE}"
fi
curl --fail --location --retry 3 --output "${WORK}/${ARCHIVE}.sha512" \
  "${ARCHIVE_BASE}/${ARCHIVE}.sha512"
(
  cd "${WORK}"
  sha512sum --check "${ARCHIVE}.sha512"
) | tee -a "${RESULT}"

tar -xzf "${WORK}/${ARCHIVE}" -C "${WORK}"
[[ -d "${CELEBORN_HOME}" ]] || fail "unexpected Celeborn binary archive layout"

cp "${CELEBORN_HOME}/conf/log4j2.xml.template" "${CELEBORN_HOME}/conf/log4j2.xml"
cat > "${CELEBORN_HOME}/conf/celeborn-defaults.conf" <<EOF
celeborn.master.host 127.0.0.1
celeborn.master.port ${MASTER_PORT}
celeborn.master.endpoints 127.0.0.1:${MASTER_PORT}
celeborn.master.heartbeat.application.timeout 20s
celeborn.worker.host 127.0.0.1
celeborn.worker.storage.dirs ${STORAGE}
celeborn.worker.disk.reserve.size 0b
celeborn.client.application.unregister.enabled true
celeborn.client.push.replicate.enabled false
EOF

export CELEBORN_HOME
export CELEBORN_CONF_DIR="${CELEBORN_HOME}/conf"
"${CELEBORN_HOME}/sbin/start-master.sh"
master_log="$(find "${CELEBORN_HOME}/logs" -type f -name '*master*.out' -o -name '*master*.log' | head -1)"
if [[ -z "${master_log}" ]] || ! wait_for_log "${master_log}" "Master.*started\|Starting RPC Server \[Master\]" 30; then
  find "${CELEBORN_HOME}/logs" -maxdepth 1 -type f -print -exec tail -100 {} \; >&2 || true
  fail "master did not become ready"
fi

"${CELEBORN_HOME}/sbin/start-worker.sh" "celeborn://127.0.0.1:${MASTER_PORT}"
worker_log="$(find "${CELEBORN_HOME}/logs" -type f -name '*worker*.out' -o -name '*worker*.log' | head -1)"
if [[ -z "${worker_log}" ]] || ! wait_for_log "${worker_log}" "Register worker successfully\|Worker started" 30; then
  find "${CELEBORN_HOME}/logs" -maxdepth 1 -type f -print -exec tail -100 {} \; >&2 || true
  fail "worker did not become ready"
fi

classpath="$(find "${CELEBORN_HOME}" -type f -name '*.jar' -print | paste -sd: -)"
[[ -n "${classpath}" ]] || fail "no Celeborn jars found in binary distribution"
mkdir -p "${WORK}/classes"
javac -cp "${classpath}" -d "${WORK}/classes" "${ROOT}/ColdDriverProviderProbe.java"
probe_classpath="${WORK}/classes:${classpath}"

java -cp "${probe_classpath}" ColdDriverProviderProbe \
  producer "127.0.0.1:${MASTER_PORT}" "${APP_ID}" "${STATE}" \
  >"${PRODUCER_LOG}" 2>&1 &
producer_pid=$!
if ! wait_for_log "${PRODUCER_LOG}" "PRODUCER_READY" 60; then
  cat "${PRODUCER_LOG}" >&2 || true
  fail "producer did not publish and control-read a complete shuffle"
fi
cat "${PRODUCER_LOG}" | grep 'PRODUCER_READY' | tee -a "${RESULT}"

before_count="$(file_count)"
before_bytes="$(file_bytes)"
[[ "${before_count}" -gt 0 ]] || fail "producer control read succeeded but no worker artifact file was observed"
log "artifactBeforeDriverLoss.fileCount=${before_count}"
log "artifactBeforeDriverLoss.totalBytes=${before_bytes}"

kill -9 "${producer_pid}"
wait "${producer_pid}" 2>/dev/null || true
producer_pid=""
log "producerLoss=SIGKILL"
log "artifactImmediatelyAfterDriverLoss.fileCount=$(file_count)"
log "artifactImmediatelyAfterDriverLoss.totalBytes=$(file_bytes)"
[[ "$(file_count)" -gt 0 ]] || fail "worker artifacts disappeared immediately with producer process loss"

set +e
timeout --signal=KILL 30s java -cp "${probe_classpath}" ColdDriverProviderProbe \
  reader "127.0.0.1:${MASTER_PORT}" "${APP_ID}" "${STATE}" \
  >"${READER_LOG}" 2>&1
reader_rc=$?
set -e
cat "${READER_LOG}" | grep -E 'READER_STARTED|EXPECTED_DISCOVERY_GAP|UNEXPECTED_COLD_READ_SUCCESS' \
  | tee -a "${RESULT}" || true

if [[ "${reader_rc}" -eq 3 ]] || grep -q 'UNEXPECTED_COLD_READ_SUCCESS' "${READER_LOG}"; then
  cat "${READER_LOG}" >&2
  fail "fresh reader unexpectedly discovered and read the old shuffle; re-evaluate the provider result"
elif [[ "${reader_rc}" -eq 0 ]] && grep -q 'EXPECTED_DISCOVERY_GAP' "${READER_LOG}"; then
  log "freshReaderResult=SUPPORTED_READ_API_COULD_NOT_DISCOVER_OLD_OUTPUT"
elif [[ "${reader_rc}" -eq 124 ]] || [[ "${reader_rc}" -eq 137 ]]; then
  log "freshReaderResult=SUPPORTED_READ_API_TIMED_OUT_WITHOUT_OLD_BYTE_READ"
else
  cat "${READER_LOG}" >&2
  fail "reader failed outside the expected discovery-gap result (exit ${reader_rc})"
fi

# The runtime uses a shortened application heartbeat timeout only to make cleanup observable in a
# bounded test. The pinned release's documented default is 300 seconds.
cleanup_deadline=$((SECONDS + 45))
while [[ "$(file_count)" -gt 0 ]] && ((SECONDS < cleanup_deadline)); do
  sleep 1
done
log "artifactAfterApplicationTimeout.fileCount=$(file_count)"
log "artifactAfterApplicationTimeout.totalBytes=$(file_bytes)"
log "configuredApplicationHeartbeatTimeout=20s"
log "documentedDefaultApplicationHeartbeatTimeout=300s"
log "PROBE_RESULT=PROVIDER_GAP"

# Keep service logs next to the result when a caller requests a persistent work directory.
log "resultFile=${RESULT}"
