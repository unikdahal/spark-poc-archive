# Native Celeborn cold-process harness

`cold-process.sh <new-evidence-directory>` runs separate baseline, producer and
replacement driver JVMs against a running Celeborn standalone lifecycle owner.
The producer exits before the replacement starts. Driver scratch is cleared
between processes; the Iceberg warehouse, manifests and external worker storage
remain available.

Required environment:

- `ICEBERG_RUNTIME_JAR`: Iceberg Spark 4.2 Scala 2.13 runtime built from
  `e76d63584d7f83b102026749e1ae0f91813cb78e`.
- `CELEBORN_RUNTIME_JAR`: Celeborn Spark 4 shaded Scala 2.13 client built from
  fork commit `edb413ee3`. Build the servers with the same Scala profile.
- `CELEBORN_RETAINED_ENDPOINT_FILE`: endpoint file created by that fork's
  standalone LifecycleManager with the retained-shuffle extension enabled.

Run from the Spark repository root. Use paths without whitespace because sbt's
runMain command parser splits the child arguments. The script compiles once with sbt and exports its resolved test classpath and
JVM options. Each role then runs in a fresh Java process. Use CI for this
expensive validation. CI compilation and execution are in progress; a committed harness alone is not
evidence of successful native recovery.

The baseline uses Spark's ordinary shuffle manager. The producer publishes its
native descriptor and the replacement must return the exact fixture with zero
target map task launches and an adopted binding before and after reading.
Source token, producer filter, missing manifest and actual Iceberg snapshot
changes must all reject recovery and launch fresh map tasks. Every role's
result digest must match the baseline. Evidence includes process identities,
jar checksums, commit, working-tree diff, counts and raw logs. The work directory
is preserved for diagnosis; the operator owns service shutdown and cleanup.

`services.py <new-directory>` owns the master, worker and standalone owner,
waits for startup/registration, invokes the drivers, and shuts down all services
in a finally block. Set `CELEBORN_HOME` to the pinned distribution. The native CI
workflow builds both dependencies and runs this entry point.

The artifact-loss control pauses after adoption, removes the exact producer
namespace's persisted worker files, then releases the reader. It requires an
observed fetch failure, invalidation, fresh target map tasks and the baseline
result. The worker and owner stay alive so ordinary recomputation remains usable.
This requires `CELEBORN_PROOF_WORKER_ROOT`, supplied by the service runner.

Two concurrent replacement JVMs also hold independent adopted claims at a shared
barrier before reading. Each must report zero map tasks and positive remote bytes.
The pass marker covers cold-process reuse, concurrent claims, identity controls
and artifact loss. Owner restart and lease expiration during an executor read
still require additional integration evidence; provider unit tests exercise
independent claim release, exact expiry, clock wraparound and owner shutdown.
