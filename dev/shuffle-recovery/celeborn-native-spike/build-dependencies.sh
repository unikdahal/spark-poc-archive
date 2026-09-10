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
root="${1:?dependency directory required}"
mkdir -p "$root"
root="$(cd "$root" && pwd)"
celeborn_commit=d154ee0b77bf4e12c9a4ea56376e39341a55f47a
iceberg_commit=e76d63584d7f83b102026749e1ae0f91813cb78e
fetch_source() {
  local name="$1" url="$2" revision="$3"
  git init -q "$root/$name"
  git -C "$root/$name" remote add origin "$url"
  git -C "$root/$name" fetch --depth=1 origin "$revision"
  git -C "$root/$name" checkout -q --detach FETCH_HEAD
  git -C "$root/$name" rev-parse HEAD > "$root/$name.commit"
}
if [[ "${2:-all}" != iceberg ]]; then
  fetch_source celeborn https://github.com/unikdahal/celeborn.git "$celeborn_commit"
  (
    cd "$root/celeborn"
    bash build/make-distribution.sh -Pspark-4.2
  ) 2>&1 | tee "$root/celeborn-build.log"
  tar -C "$root/celeborn" -czf "$root/celeborn-dist.tgz" dist
fi
if [[ "${2:-all}" != celeborn ]]; then
  fetch_source iceberg https://github.com/apache/iceberg.git "$iceberg_commit"
  (
    cd "$root/iceberg"
    ./gradlew --no-daemon -DsparkVersions=4.2 \
      :iceberg-spark:iceberg-spark-runtime-4.2_2.13:shadowJar
  ) 2>&1 | tee "$root/iceberg-build.log"
  mapfile -t jars < <(find "$root/iceberg/spark/v4.2/spark-runtime/build/libs" \
    -maxdepth 1 -name 'iceberg-spark-runtime-4.2_2.13-*.jar' \
    ! -name '*-sources.jar' ! -name '*-javadoc.jar')
  [[ ${#jars[@]} == 1 ]]
  cp "${jars[0]}" "$root/iceberg-runtime.jar"
fi
