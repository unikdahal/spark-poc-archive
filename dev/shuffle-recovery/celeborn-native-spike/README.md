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
  fork commit `4f0f787cb`. Build the servers with the same Scala profile.
- `CELEBORN_RETAINED_ENDPOINT_FILE`: endpoint file created by that fork's
  standalone LifecycleManager with the retained-shuffle extension enabled.

Run from the Spark repository root. Use paths without whitespace because sbt's
runMain command parser splits the child arguments. The script uses incremental
sbt compilation, but deliberately forks each driver independently. Use CI for
this expensive validation. No build or native execution has yet validated these
sources.

The baseline uses Spark's ordinary shuffle manager. The producer publishes its
native descriptor and the replacement must return the exact fixture with zero
target map task launches and an adopted binding before and after reading.
Source token, producer filter, missing manifest and actual Iceberg snapshot
changes must all reject recovery and launch fresh map tasks. Every role's
result digest must match the baseline. Evidence includes process identities,
jar checksums, commit, working-tree diff, counts and raw logs. The work directory
is preserved for diagnosis; the operator owns service shutdown and cleanup.

The pass marker covers cold-process reuse and identity controls only. It does
not cover worker artifact loss after adoption, owner restart, lease expiration,
or concurrent replacements. Service startup/build orchestration and those
failure controls remain to be added before calling this an end-to-end proof.
