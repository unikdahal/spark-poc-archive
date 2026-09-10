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

: "${ICEBERG_RUNTIME_JAR:?source-built Iceberg runtime jar is required}"
: "${CELEBORN_RUNTIME_JAR:?source-built Celeborn Spark 4 shaded jar is required}"
: "${CELEBORN_RETAINED_ENDPOINT_FILE:?running standalone owner endpoint file is required}"
[[ -f "$ICEBERG_RUNTIME_JAR" && -f "$CELEBORN_RUNTIME_JAR" ]]
[[ -f "$CELEBORN_RETAINED_ENDPOINT_FILE" ]]
evidence_dir="${1:?new evidence directory is required}"
mkdir "${evidence_dir}"
evidence_dir="$(cd "${evidence_dir}" && pwd)"
work_dir="$(mktemp -d "${TMPDIR:-/tmp}/celeborn-cold.XXXXXX")"
# Preserve the warehouse and manifests for diagnosis, including on failure.
printf '%s\n' "${work_dir}" > "${evidence_dir}/work-directory.txt"
export SPARK_RECOVERY_TESTED_COMMIT="$(git rev-parse HEAD)"
export SPARK_SHUFFLE_RECOVERY_TEST_ICEBERG_WAREHOUSE="file://${work_dir}/warehouse"
export SPARK_SHUFFLE_RECOVERY_TEST_PRODUCER_FILTER=false
export SPARK_LOCAL_DIRS="${work_dir}/scratch"
mkdir "${work_dir}/manifests"
sha256sum "$ICEBERG_RUNTIME_JAR" "$CELEBORN_RUNTIME_JAR" > "${evidence_dir}/jars.sha256"
git diff --binary HEAD > "${evidence_dir}/working-tree.patch"
entry=org.apache.spark.shuffle.celeborn.ShuffleRecoveryCelebornColdProcess
setup=org.apache.spark.sql.execution.exchange.ShuffleRecoveryIcebergColdSourceSetup
group="native-$(basename "${work_dir}")"

run_main() {
  local label="$1" command="$2"
  # Each sbt invocation forks a fresh driver and returns only after that driver exits.
  rm -rf "${work_dir}/scratch"
  mkdir "${work_dir}/scratch"
  timeout --kill-after=30s 20m ./build/sbt -Phadoop-3 -Phive 'project sql' \
    'set Test / unmanagedSourceDirectories ++= Seq("java", "scala").map(lang => file(sys.props("user.dir")) / "dev/shuffle-recovery/iceberg-source-spike/src/main" / lang)' \
    'set Test / unmanagedSourceDirectories += file(sys.props("user.dir")) / "dev/shuffle-recovery/celeborn-native-spike/src/main/scala"' \
    'set Test / unmanagedJars ++= Seq("ICEBERG_RUNTIME_JAR", "CELEBORN_RUNTIME_JAR").map(key => file(sys.env(key)))' \
    'set Test / run / fork := true' \
    'set Test / javaOptions += "-Dspark.shuffle.useOldFetchProtocol=false"' \
    "Test/runMain ${command}" 2>&1 | tee "${evidence_dir}/${label}.log"
}
run_child() {
  local label="$1" role="$2" control="$3"
  run_main "$label" "${entry} ${role} ${work_dir}/manifests ${evidence_dir}/${label}.properties ${group} ${control}"
}
run_main create "${setup} create ${evidence_dir}/snapshot-before.txt"
run_child baseline baseline none
run_child producer producer none
run_child replacement replacement none
for control in source-token manifest-missing producer-filter; do
  export SPARK_SHUFFLE_RECOVERY_TEST_PRODUCER_FILTER=false
  if [[ "$control" == producer-filter ]]; then
    export SPARK_SHUFFLE_RECOVERY_TEST_PRODUCER_FILTER=true
  fi
  run_child "$control" replacement "$control"
done
export SPARK_SHUFFLE_RECOVERY_TEST_PRODUCER_FILTER=false
run_main rewrite "${setup} rewrite ${evidence_dir}/snapshot-after.txt"
run_child source-snapshot replacement source-snapshot

python3 - "${evidence_dir}" <<'CHECK'
import sys
from pathlib import Path

root = Path(sys.argv[1])
processes = set()
records = {}
for name in ("baseline", "producer", "replacement", "source-token",
             "manifest-missing", "producer-filter", "source-snapshot"):
    row = dict(line.split("=", 1) for line in
               (root / (name + ".properties")).read_text().splitlines())
    process = (row["pid"], row["started"])
    assert process not in processes, "driver JVM was reused"
    processes.add(process)
    assert row["rowCount"] == "32", name
    records[name] = row
baseline = records["baseline"]
for name, row in records.items():
    assert row["resultDigest"] == baseline["resultDigest"], name
    assert row["testedCommit"] == baseline["testedCommit"], name
    if name == "replacement":
        assert row["adopted"] == "true" and row["offered"] == "true"
        assert row["mapTaskCount"] == "0"
    else:
        assert row["adopted"] == "false" and int(row["mapTaskCount"]) > 0, name
assert (root / "snapshot-before.txt").read_text() != (
    root / "snapshot-after.txt").read_text()
(root / "decision.txt").write_text("NATIVE_COLD_PROCESS_IDENTITY_CONTROLS_PASS\n")
CHECK
