# Apache Celeborn 0.6.3 cold-driver provider assessment

## Decision

**Provider result: GAP / NO-GO for the required cold-driver recovery gate.**

Apache Celeborn 0.6.3 can keep shuffle bytes outside the Spark driver, but its supported reduce-read bootstrap depends on control metadata owned by the application-scoped `LifecycleManager`. A replacement driver cannot reconstruct that metadata through a documented service API after the producer `LifecycleManager` is gone. Authentication strengthens the same boundary: the application shared secret is generated during `LifecycleManager` registration and is not a documented durable credential that a fresh driver may recover.

This is an adverse research result, not a successful Gate A result. It must not be interpreted as deployable cross-driver shuffle recovery, and it does not authorize a Spark adapter that copies producer heap metadata, preserves the old `LifecycleManager`, disables security, or indefinitely suppresses normal cleanup.

No alternate provider is selected by this assessment. Choosing one requires an explicit follow-up design decision rather than silently changing the provider under evaluation.

## Pinned release

| Item | Value |
| --- | --- |
| Provider | Apache Celeborn |
| Release | `0.6.3` |
| Release date | 2026-04-23 |
| Git tag | `v0.6.3` |
| Tag commit | `f583b73d292afdfeed865e20610838121c9db9cf` |
| Binary | `apache-celeborn-0.6.3-bin.tgz` |
| Binary integrity | ASF-published `.sha512`, verified by the executable harness before startup |
| Spark dependency added | none |
| Provider control plane added | none |

The source verifier checks out the exact tag commit and fails if the ownership/lifecycle facts relied on by this report no longer match the pinned source.

## Process topology

The executable probe uses four independently relevant process roles:

```text
Celeborn Master
      |
      +---- Celeborn Worker ---- durable worker file(s)
      |
producer JVM
  +-- producer LifecycleManager
  +-- producer ShuffleClient
      |
      X  SIGKILL: producer JVM and LifecycleManager disappear together
      |
fresh reader JVM
  +-- new LifecycleManager
  +-- new ShuffleClient
```

The producer writes one map to one reducer, calls `mapperEnd`, and performs a control read before it is considered complete. The evidence file handed to the reader contains only non-secret facts: application ID, shuffle/partition IDs, payload length/digest, producer control-read counter and the producer endpoint for diagnostics. It does **not** contain reducer file groups, `PartitionLocation`s, client objects, generated secrets, or serialized provider control state.

The replacement reader creates its own new `LifecycleManager`. It is never pointed at the dead producer endpoint.

## Metadata ownership map

### LifecycleManager / application process

In the pinned release, `LifecycleManager` is an application singleton `RpcEndpoint`. It owns or coordinates the state required to transform an application shuffle ID into readable worker locations, including:

- allocated worker / partition-location state;
- split/location evolution;
- registered shuffle state;
- `CommitManager` and completed partition metadata;
- stage-end state used to service reducer file-group requests;
- the application heartbeat process;
- when authentication is enabled, the application shared-secret bootstrap.

`CommitManager` keeps `committedPartitionInfo` in a process-local concurrent map. The reduce commit handler also keeps stage-end, mapper-attempt and file-group request state in the same application process.

### Master

The Master owns cluster/application registration and timeout state, worker registration, registered application/shuffle bookkeeping, and—when authentication is enabled—the application secret registered through the secret registry. It does not expose the completed reducer file-group catalog used by `ShuffleClient.readPartition` as a cold-reader discovery service in 0.6.3.

### Worker

Workers own shuffle data files and serve fetch requests once a reader has valid location/stream metadata. Durable worker bytes therefore do not, by themselves, make a shuffle cold-discoverable.

### Reader bootstrap

`ShuffleClient` first checks its local reducer-file-group cache. On a miss it sends `GetReducerFileGroup` to its configured `LifecycleManager`. The response contains the transfer metadata needed by the supported reduce path:

- reducer `PartitionLocation` groups;
- mapper attempts;
- partition IDs;
- push-failed batch metadata;
- optional broadcast bytes;
- serialization version.

There is no documented fallback from a dead application `LifecycleManager` to Master/Worker discovery for an already completed shuffle in the pinned release.

## Completion certification

The supported reduce-shuffle path uses mapper completion and final commit handling before stage end. The probe deliberately calls `mapperEnd` for the only mapper and then performs a producer-side read of the exact payload. That control read is the runtime certification that the produced shuffle was complete and readable before driver loss.

This report does not reinterpret a partially written worker file as a completed reusable shuffle.

## Lifecycle and cleanup

The relevant lifecycle is application-scoped rather than recovery-group-scoped:

1. `LifecycleManager` sends application heartbeats to Master.
2. Normal client shutdown has `celeborn.client.application.unregister.enabled=true` by default and sends `ApplicationLost`.
3. If the producer disappears without unregistering, Master detects application heartbeat expiry.
4. The pinned documented default `celeborn.master.heartbeat.application.timeout` is **300 seconds**.
5. Application loss removes application/shuffle metadata and permits worker/remote-storage cleanup.

The runtime harness shortens the heartbeat timeout to 20 seconds only to make cleanup observable in bounded CI. It separately records the 300-second release default. A short/long timeout is not a read lease and is not treated as identity material.

Disabling application unregister would only postpone cleanup until heartbeat expiry. It would not recreate the dead `LifecycleManager` file-group catalog and therefore is not a solution to cold discovery.

## Authentication and authorization

Celeborn 0.6.3 documents SASL application authentication and TLS network encryption. Both are optional and disabled by default in the release documentation.

When authentication is enabled:

- the `LifecycleManager` generates the application shared secret during registration;
- Master persists application metadata/secret state;
- subsequent application connections authenticate with that shared secret;
- Master retains the first application metadata registration for an application ID;
- application loss removes the registration/secret.

The required recovery experiment forbids copying the secret out of producer memory as a substitute for a supported fresh-client identity mechanism. The pinned release does not document a fresh-driver reattachment/credential-recovery protocol that simultaneously preserves the old completed shuffle and grants a newly started `LifecycleManager` access to it.

For that reason, an authenticated cold read is blocked before provider-native data reading can become a valid supported operation. The runtime probe isolates the independent metadata-discovery failure without weakening provider security. This report does not claim that disabling authentication passes the authorization requirement.

## Attempt, group and artifact lifetime

Celeborn's 0.6.3 lifecycle is centered on application ID plus shuffle ID and its application-scoped `LifecycleManager`. The release does not expose the recovery semantics required here as separate supported concepts:

- replacement-driver attempt identity distinct from a reusable recovery group;
- immutable artifact incarnation identity independent of the current application control process;
- group-scoped pin/retention independent of attempt cleanup;
- a finite authenticated read lease that can survive one driver attempt and later expire;
- conditional retirement of exactly one corrupt/lost immutable incarnation.

Reusing the same application/shuffle ID is therefore not equivalent to proving an attempt-independent durable artifact contract.

## Empty reads and zero-byte output

`GetReducerFileGroupResponse` supports empty maps/arrays, so the wire/control types can represent empty file groups. That does not solve cold discovery: the authoritative response is still produced by the application `LifecycleManager` state that disappears with the producer driver.

The executable probe uses a non-empty payload specifically so an empty response cannot be mistaken for successful old-byte recovery. A production design would need an explicit certified-empty artifact state rather than inferring emptiness from missing metadata.

## Encryption compatibility

The pinned 0.6.3 security documentation covers SASL authentication and TLS encryption in transit, including the application-to-LifecycleManager and application-to-service transport namespaces. The cold-discovery failure occurs before a fresh reader obtains the transfer metadata needed to open the old partition, so TLS compatibility does not repair the lifecycle gap.

No encryption-at-rest recovery claim is made by this assessment. Any future provider candidate that encrypts shuffle bytes at rest must prove that required key/material lifetime is independent of the dead producer attempt and can be reauthorized without copying producer secrets.

## Pin versus copy

No supported 0.6.3 API was found that pins a completed shuffle artifact for a replacement driver independently of the application heartbeat/unregister lifecycle. The only observed survival window after abrupt producer loss is the ordinary application-timeout interval before cleanup.

That window is not a recovery retention contract. Extending it indefinitely or disabling cleanup would be an unexplained operational workaround and still would not provide the missing file-group discovery path.

The design must pin/rebind existing immutable output rather than copy shuffle bytes. This release does not expose that operation as the required cold-driver capability.

## Provider-service failure assumptions

The experiment keeps Master and Worker alive while only the producer driver/LifecycleManager is lost. It does **not** test or claim:

- Master failover;
- Worker process loss/restart;
- replicated-worker failover;
- remote-storage failover;
- cross-cluster disaster recovery.

Those are separate provider-service availability questions. Failure of the narrower driver-loss test is sufficient to reject the pinned release for this gate without making broader claims.

## Capability table

| Capability required by the recovery gate | Celeborn 0.6.3 result | Evidence / reason |
| --- | --- | --- |
| Provider-owned shuffle bytes | yes | Worker/remote storage owns data outside Spark driver |
| Complete-output certification | yes, within live application lifecycle | mapper completion/final commit followed by successful producer control read |
| Fresh process can start a new client | yes | New `LifecycleManager` / `ShuffleClient` can be constructed |
| Fresh process can discover completed old output after producer LM loss | **no supported path found** | reducer file-group discovery RPC is served by the application `LifecycleManager`; completed metadata is process-local |
| Old worker bytes survive immediate producer SIGKILL | runtime probe records this separately | proves bytes and discovery metadata have different lifetimes |
| Authenticated fresh-driver reattachment to the old artifact | **no supported path found** | application secret is generated during LM registration; no documented recovery credential / reattachment protocol |
| Attempt identity distinct from recovery-group lifetime | **no** | lifecycle is application/shuffle scoped |
| Finite read lease independent of application heartbeat | **no** | application heartbeat timeout/unregister controls cleanup; no independent read lease/pin API |
| Pin existing output without byte copy | **no supported API found** | output remains tied to application lifecycle |
| Empty-output representation | partially | response types can be empty; no attempt-independent certified-empty discovery |
| Network encryption compatibility | yes for normal live-client path | TLS namespaces documented; does not solve cold bootstrap |
| Provider-service failover | not tested | explicitly out of this driver-loss result |

## Precise provider capability gap

A provider that satisfies this recovery gate needs a supported capability equivalent to all of the following:

1. **Service-owned completed-shuffle catalog.** Completion metadata and transfer-unit descriptors must survive the producer application process and be discoverable without the old `LifecycleManager` heap.
2. **Authenticated reattachment.** A new driver/client must authenticate as an authorized reader of an existing immutable artifact without copying a producer-generated secret from dead-driver memory. Attempt identity and recovery-group/artifact authority must be separable.
3. **Independent finite retention/read lease.** Completed output must be pin-able for a bounded period independent of application heartbeat/unregister, with ordinary attempt cleanup removing only the attempt binding.
4. **Immutable artifact identity and exact retirement.** Reattachment must identify the exact immutable incarnation so authoritative loss/corruption can retire only that incarnation; timeouts or uncertainty must not delete healthy data.
5. **Provider-native transfer metadata.** A fresh client must obtain bounded worker/storage handles and completion facts through the provider service and then read through the provider's normal data path.

For Celeborn 0.6.3 this is not a small Spark adapter. It spans LifecycleManager deployment/ownership, persistent service metadata, authentication/secret lifecycle, application cleanup semantics, worker retention and client read bootstrap. It is appropriately treated as an upstream provider design change rather than hidden Spark-side state preservation.

Newer development trees contain a separately deployable `lifecycle-manager` module, which is evidence that provider architecture is evolving, but that module is absent from the pinned `v0.6.3` release. This assessment does not claim that the newer module satisfies the requirements and does not claim Celeborn maintainer agreement.

## Reproduction

Source ownership/lifecycle audit:

```bash
bash docs/shuffle-recovery/experimental/celeborn-0.6.3/verify-pinned-provider-source.sh
```

Independent-process provider probe:

```bash
bash docs/shuffle-recovery/experimental/celeborn-0.6.3/run-cold-driver-gap-probe.sh
```

The provider probe downloads the ASF binary, verifies the ASF SHA-512 file, starts one Master and one Worker, compiles the small probe against the release jars, starts the producer JVM, certifies a control read, SIGKILLs the producer, checks worker-file survival, starts the fresh reader JVM, and observes cleanup after a shortened application timeout.

## Executed evidence

The executable provider run is carried by the dedicated prototype workflow during development. The final version of this report records the successful evidence run ID and exact observed counters before merge. The normal prototype workflow is restored and rerun on the final PR head; provider-specific CI scaffolding is not retained as a permanent dependency.

## Maintainer feedback

No Celeborn maintainer feedback was obtained as part of this assessment. No statement in this report should be read as project agreement or a commitment that a future Celeborn release will provide the missing capability.
