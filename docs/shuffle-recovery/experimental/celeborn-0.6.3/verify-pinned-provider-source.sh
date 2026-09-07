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

VERSION="v0.6.3"
EXPECTED_COMMIT="f583b73d292afdfeed865e20610838121c9db9cf"
WORK="${CELEBORN_SOURCE_AUDIT_DIR:-$(mktemp -d)}"
SOURCE="${WORK}/celeborn"

if [[ ! -d "${SOURCE}/.git" ]]; then
  git clone --quiet --filter=blob:none --no-checkout https://github.com/apache/celeborn.git "${SOURCE}"
fi

git -C "${SOURCE}" fetch --quiet --depth=1 origin "refs/tags/${VERSION}:refs/tags/${VERSION}"
actual_commit="$(git -C "${SOURCE}" rev-parse "${VERSION}^{commit}")"
if [[ "${actual_commit}" != "${EXPECTED_COMMIT}" ]]; then
  echo "unexpected ${VERSION} commit: ${actual_commit}" >&2
  exit 1
fi
git -C "${SOURCE}" checkout --quiet --detach "${EXPECTED_COMMIT}"

grep -Fq 'class LifecycleManager(val appUniqueId: String, val conf: CelebornConf) extends RpcEndpoint' \
  "${SOURCE}/client/src/main/scala/org/apache/celeborn/client/LifecycleManager.scala"
grep -Fq 'val committedPartitionInfo = new CommittedPartitionInfo' \
  "${SOURCE}/client/src/main/scala/org/apache/celeborn/client/CommitManager.scala"
grep -Fq 'case GetReducerFileGroup(' \
  "${SOURCE}/client/src/main/scala/org/apache/celeborn/client/LifecycleManager.scala"
grep -Fq 'commitManager.handleGetReducerFileGroup(context, shuffleId, serdeVersion)' \
  "${SOURCE}/client/src/main/scala/org/apache/celeborn/client/LifecycleManager.scala"
grep -Fq 'val appSecret = createSecret()' \
  "${SOURCE}/client/src/main/scala/org/apache/celeborn/client/LifecycleManager.scala"
grep -Fq 'applicationMetas.putIfAbsent(applicationMeta.appId(), applicationMeta);' \
  "${SOURCE}/master/src/main/java/org/apache/celeborn/service/deploy/master/clustermeta/AbstractMetaManager.java"
grep -Fq 'registeredAppAndShuffles.remove(appId);' \
  "${SOURCE}/master/src/main/java/org/apache/celeborn/service/deploy/master/clustermeta/AbstractMetaManager.java"
grep -Fq 'if (shuffleIds == null || !shuffleIds.contains(shuffleId)) {' \
  "${SOURCE}/master/src/main/scala/org/apache/celeborn/service/deploy/master/Master.scala"
grep -Fq '| celeborn.client.application.unregister.enabled | true |' \
  "${SOURCE}/docs/configuration/client.md"
grep -Fq '| celeborn.master.heartbeat.application.timeout | 300s |' \
  "${SOURCE}/docs/configuration/master.md"
grep -Fq 'SASL is leveraged by Celeborn to authenticate requests from an application' \
  "${SOURCE}/docs/security.md"
grep -Fq 'The `shared secret`, which is generated as part of application registration' \
  "${SOURCE}/docs/security.md"
grep -Fq 'sends GetReducerFileGroup to `LifecycleManager`' \
  "${SOURCE}/docs/developers/shuffleclient.md"

# A separately deployable LifecycleManager appears on newer development branches, but it is not a
# capability of the pinned stable release being evaluated here.
if git -C "${SOURCE}" cat-file -e "${VERSION}:lifecycle-manager" 2>/dev/null; then
  echo "unexpected standalone lifecycle-manager module in ${VERSION}" >&2
  exit 1
fi

cat <<EOF
CELEBORN_PROVIDER_SOURCE_AUDIT=PASS
version=${VERSION}
commit=${actual_commit}
lifecycleManagerOwnership=application-process RpcEndpoint
completedFileGroupOwnership=LifecycleManager CommitManager heap
readerBootstrap=GetReducerFileGroup to LifecycleManager
authSecretOwnership=LifecycleManager-generated; Master persists first app registration
normalClientUnregister=true
applicationHeartbeatTimeoutDefault=300s
applicationLossRemovesRegisteredShuffles=true
workerHeartbeatExpiresUnknownShuffleKeys=true
standaloneLifecycleManagerModule=false
EOF
