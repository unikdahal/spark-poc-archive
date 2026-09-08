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

: "${CANDIDATE_SHA:?}"
: "${BASE_SHA:?}"
: "${VALIDATION_MODE:?}"
: "${EVENT_NAME:?}"
: "${HEAD_REF:?}"
: "${INTEGRATION_BRANCH:?}"
: "${EVIDENCE_DIR:?}"
: "${PREFLIGHT_RESULT:?}"
: "${EXPECT_QUALITY:?}"
: "${QUALITY_RESULT:?}"
: "${EXPECT_CORE:?}"
: "${CORE_RESULT:?}"
: "${EXPECT_SQL:?}"
: "${SQL_RESULT:?}"

mkdir -p "${EVIDENCE_DIR}"
{
  echo "candidate_kind=experimental-tree"
  echo "candidate_sha=${CANDIDATE_SHA}"
  echo "integration_base_sha=${BASE_SHA}"
  echo "validation_mode=${VALIDATION_MODE}"
  echo "workflow_run_id=${GITHUB_RUN_ID:-local}"
  echo "preflight_job=${PREFLIGHT_RESULT}"
  echo "quality_job=${QUALITY_RESULT}"
  echo "quality_lint=${QUALITY_LINT:-}"
  echo "quality_license=${QUALITY_LICENSE:-}"
  echo "core_job=${CORE_RESULT}"
  echo "core_compilation=${CORE_COMPILATION:-}"
  echo "core_tests=${CORE_TESTS:-}"
  echo "core_proof=${CORE_PROOF:-}"
  echo "sql_job=${SQL_RESULT}"
  echo "sql_compilation=${SQL_COMPILATION:-}"
  echo "sql_tests=${SQL_TESTS:-}"
  echo "sql_proof=${SQL_PROOF:-}"
} > "${EVIDENCE_DIR}/status.env"
cp "${EVIDENCE_DIR}/status.env" "${EVIDENCE_DIR}/provenance.env"

freshness=success
integration_current="$(git ls-remote origin "refs/heads/${INTEGRATION_BRANCH}" | awk 'NR == 1 {print $1}')"
if [[ "${integration_current}" != "${BASE_SHA}" ]]; then
  echo "STALE BASE: tested against ${BASE_SHA}, current ${INTEGRATION_BRANCH} is ${integration_current}" >&2
  freshness=failed
fi
if [[ "${EVENT_NAME}" == pull_request ]]; then
  remote="$(git ls-remote origin "refs/heads/${HEAD_REF}" | awk 'NR == 1 {print $1}')"
  subject=branch
else
  remote="$(git ls-remote origin "refs/tags/${HEAD_REF}^{}" | awk 'NR == 1 {print $1}')"
  [[ -n "${remote}" ]] || remote="$(git ls-remote origin "refs/tags/${HEAD_REF}" | awk 'NR == 1 {print $1}')"
  subject=tag
fi
if [[ ! "${remote}" =~ ^[0-9a-f]{40}$ || "${remote}" != "${CANDIDATE_SHA}" ]]; then
  echo "STALE CANDIDATE: tested ${CANDIDATE_SHA}, current ${HEAD_REF} is ${remote:-unresolved}" >&2
  freshness=failed
fi
{
  echo "candidate_ref=${HEAD_REF}"
  echo "candidate_ref_type=${subject}"
  echo "current_ref_sha=${remote:-unresolved}"
  echo "current_integration_sha=${integration_current:-unresolved}"
  echo "freshness=${freshness}"
} >> "${EVIDENCE_DIR}/provenance.env"
echo "freshness=${freshness}" >> "${EVIDENCE_DIR}/status.env"
bash .github/scripts/shuffle-recovery-ci-evidence.sh checksum-tree "${EVIDENCE_DIR}"

failed=false
[[ "${PREFLIGHT_RESULT}" == success ]] || failed=true
if [[ "${EXPECT_QUALITY}" == true ]]; then [[ "${QUALITY_RESULT}" == success ]] || failed=true; else [[ "${QUALITY_RESULT}" == skipped ]] || failed=true; fi
if [[ "${EXPECT_CORE}" == true ]]; then [[ "${CORE_RESULT}" == success ]] || failed=true; else [[ "${CORE_RESULT}" == skipped ]] || failed=true; fi
if [[ "${EXPECT_SQL}" == true ]]; then [[ "${SQL_RESULT}" == success ]] || failed=true; else [[ "${SQL_RESULT}" == skipped ]] || failed=true; fi
[[ "${freshness}" == success ]] || failed=true
[[ "${failed}" == false ]]
