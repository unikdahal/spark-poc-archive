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

repo_root="$(git rev-parse --show-toplevel)"
cd "${repo_root}"

readonly iceberg_version="1.11.0"
readonly iceberg_artifact="iceberg-spark-runtime-4.0_2.13"
readonly iceberg_jar_name="${iceberg_artifact}-${iceberg_version}.jar"
readonly iceberg_url="https://repo.maven.apache.org/maven2/org/apache/iceberg/${iceberg_artifact}/${iceberg_version}/${iceberg_jar_name}"

work_dir="$(mktemp -d "${TMPDIR:-/tmp}/shuffle-recovery-iceberg-source.XXXXXX")"
trap 'rm -rf "${work_dir}"' EXIT

export ICEBERG_VERSION="${iceberg_version}"
export ICEBERG_RUNTIME_JAR="${work_dir}/${iceberg_jar_name}"

evidence_path="${1:-${work_dir}/iceberg-source-evidence.tsv}"
mkdir -p "$(dirname "${evidence_path}")"

curl --fail --location --retry 3 --retry-all-errors \
  --output "${ICEBERG_RUNTIME_JAR}" "${iceberg_url}"
sha512sum "${ICEBERG_RUNTIME_JAR}"

./build/sbt -Phadoop-3 -Phive \
  "project sql" \
  'set Test / unmanagedSourceDirectories += file(sys.props("user.dir")) / "dev/shuffle-recovery/iceberg-source-spike/src/main/java"' \
  'set Test / unmanagedJars += file(sys.env("ICEBERG_RUNTIME_JAR"))' \
  "Test / compile" \
  "Test / runMain org.apache.iceberg.spark.source.ShuffleRecoveryIcebergSourceSpike ${evidence_path}"

test -s "${evidence_path}"
grep -F $'result\tPASS' "${evidence_path}"
grep -F $'decision\tRESOLVED_SCAN_CERTIFICATION_FEASIBLE_WITH_PRIVATE_ICEBERG_HOOKS' "${evidence_path}"
cat "${evidence_path}"
