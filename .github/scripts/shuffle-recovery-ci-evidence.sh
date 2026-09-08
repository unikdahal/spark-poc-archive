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

command="${1:-}"
shift $(( $# > 0 ? 1 : 0 ))

case "${command}" in
  resolve-mode)
    event_name="${1:-}"
    ref_name="${2:-}"
    actual="${3:-}"
    case "${event_name}" in
      pull_request)
        mode=focused
        requested="${actual}"
        ;;
      push)
        if [[ "${ref_name}" =~ ^shuffle-recovery-ci/(full|evidence)/([0-9a-fA-F]{40})$ ]]; then
          mode="${BASH_REMATCH[1]}"
          requested="${BASH_REMATCH[2]}"
        else
          echo "unsupported shuffle recovery tag: ${ref_name:-<empty>}" >&2
          exit 1
        fi
        ;;
      *)
        echo "unsupported shuffle recovery event: ${event_name:-<empty>}" >&2
        exit 1
        ;;
    esac
    bash "${BASH_SOURCE[0]}" validate-candidate "${mode}" "${requested}" "${actual}"
    echo "validation_mode=${mode}"
    echo "requested_sha=${requested}"
    ;;

  validate-candidate)
    mode="${1:-}"
    requested="${2:-}"
    actual="${3:-}"
    case "${mode}" in focused|full|evidence) ;; *) exit 2 ;; esac
    if [[ "${mode}" == full || "${mode}" == evidence ]]; then
      if [[ ! "${requested}" =~ ^[0-9a-fA-F]{40}$ ]]; then
        echo "full/evidence mode requires a full 40-character candidate SHA" >&2
        exit 1
      fi
    elif [[ -z "${requested}" ]]; then
      exit 0
    elif [[ ! "${requested}" =~ ^[0-9a-fA-F]{40}$ ]]; then
      echo "candidate SHA must be a full 40-character SHA when supplied" >&2
      exit 1
    fi
    if [[ "${requested,,}" != "${actual,,}" ]]; then
      echo "checked out ${actual}, expected ${requested}" >&2
      exit 1
    fi
    ;;

  require-suite)
    module="${1:?module is required}"
    suite="${2:?suite is required}"
    short="${suite##*.}"
    match="$(find "${module}/src/test" -type f -name "${short}.scala" -print -quit 2>/dev/null || true)"
    if [[ -z "${match}" ]]; then
      echo "required suite ${suite} is missing from ${module}" >&2
      exit 1
    fi
    printf '%s\n' "${match}"
    ;;

  require-report)
    module="${1:?module is required}"
    suite="${2:?suite is required}"
    short="${suite##*.}"
    report="$(find "${module}" -type f -path '*/test-reports/*' -name "*${short}*.xml" -print -quit 2>/dev/null || true)"
    if [[ -z "${report}" || ! -s "${report}" ]]; then
      echo "required suite ${suite} produced no non-empty test report" >&2
      exit 1
    fi
    printf '%s\n' "${report}"
    ;;

  checksum-tree)
    directory="${1:?directory is required}"
    if [[ ! -d "${directory}" ]]; then
      echo "evidence directory does not exist: ${directory}" >&2
      exit 1
    fi
    (
      cd "${directory}"
      mapfile -d '' files < <(find . -type f ! -name checksums.sha256 -print0 | sort -z)
      if [[ ${#files[@]} -eq 0 ]]; then
        echo "no evidence files available for checksum" >&2
        exit 1
      fi
      : > checksums.sha256
      for file in "${files[@]}"; do
        sha256sum "${file#./}" >> checksums.sha256
      done
      test -s checksums.sha256
    )
    ;;

  *)
    echo "unsupported evidence command: ${command:-<empty>}" >&2
    exit 2
    ;;
esac
