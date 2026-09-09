# Owned completed-shuffle PoC

The active integration candidate is `spip/poc-owned-end-to-end`. Work is driven by executable
behavior and evidence, not by the old issue boundaries. The frozen Phase 0 evidence remains
unchanged. Open-PR source compatibility and evidence-validation work is preserved in merge history.

## Generic extension boundaries

Iceberg and Celeborn are the primary integration targets, in different roles: the source connector
certifies the resolved read, and the shuffle provider retains and serves completed output. Core
must not select behavior by either product name. Other connectors and providers should implement
the same eventual contracts. NFS and the built-in Range source are validation fixtures.

The current implementation connects the full canonical computation identity to the existing
manifest store and adoption-validation boundary. A versioned identity envelope retains the legacy
Phase 0 encoding and admits the bounded canonical encoding, including opaque source tokens and
provider format IDs. Discovery compares the full payload after a digest lookup. Preparation checks
that the supplied identity agrees with the current resolved inputs and dependency shape.
The SQL builder takes the selected provider's read-format ID explicitly; it does not instantiate or
choose a provider. A format change changes the identity even when the source and SQL plan agree.

The shared-filesystem fixture now reconstructs a Range/Project/Shuffle computation in each child
JVM and uses that canonical identity for publication and recovery. A changed producer filter must
miss even when the source token and expected rows are unchanged. Core tests also exercise a
non-reference provider format identifier; that proves generic identity handling, not an implemented
alternative provider read path. The retained map-descriptor format and runtime provider integration
still need to become a complete provider contract. No generic public SPI is declared finished here.

## What the current candidate is establishing

1. Exact-candidate Core and SQL correctness, including independently launched cold/healing JVMs.
2. An out-of-tree Iceberg runtime built at a pinned source revision, with catalog compatibility,
   exact row-value checks, resolved scan certificates and conservative unsupported results.
3. A shared-filesystem experiment using loopback NFS and separate Spark executor processes. The
   selected map winners are fetched from those executors through Spark's block transport, copied
   into the retained provider namespace, and read by a fresh replacement application.

The shared-filesystem experiment is deliberately small. NFS `sync` exports and atomic filesystem
operations supply the storage substrate; the existing experimental provider supplies the format.
Only the shared namespace survives between producer and replacement; Spark scratch directories are
removed. The test uses a single ephemeral CI VM and does not establish cross-host performance,
Kerberos, server failover, authenticated recovery leases, or production retention behavior.

Publication copies and the driver-served adopted-read path remain explicit limitations of this
experiment. They must be replaced or fully costed before claiming a scalable implementation. The
experiment does not turn the reference provider into a completed production SPI.

## Source semantics

The unchanged resolved scan must reproduce its certificate. A changed scan, snapshot, projection,
filter or decomposition may refuse reuse. Schema evolution is checked against exact expected row
values; changing unprojected metadata may conservatively change the scan certificate. No unrelated
schema-evolution cache-hit guarantee is made.

The source spike rejects delete-bearing tasks, nested schemas, non-null field defaults and
unreviewed filter terms/literals. Its bounded digest counts scalar and array bytes together; source
planning memory is separate from additional certificate work. The real source and durable shuffle
experiment are not yet one automatically integrated replay query.

The delete refusal is exercised with an actual merge-on-read row deletion. Certification must
report a delete-bearing task; ordinary execution must return every remaining expected row with its
correct payload. A metadata-only whole-file removal does not satisfy that check.

## Current evidence

Candidate `89f3c0673bf764f7a430c9886e45d39bcc978129` passed the complete
[Actions gate](https://github.com/unikdahal/spark/actions/runs/34274067280): routing, lint/license,
Core, SQL, pinned Iceberg conformance and the shared-filesystem mechanism experiment.

The downloaded SQL artifact validates 39 cold child records, including nine successful replacements
and ten negative controls, plus three healing child records against that exact candidate. Its NFS
experiment mounted NFS 4.2, recovered 32 rows with the baseline digest, ran zero selected map tasks,
and read 1,152 provider bytes across 30 nonempty blocks under a new shuffle ID. This is a small
mechanism result, not a performance measurement or proof of production durability.

The Iceberg artifact records a conservative certificate miss after unprojected schema evolution,
preserved exact values, repeated/pinned snapshot identity, latest-snapshot advancement, changed split
decomposition and preserved ordinary source errors. The earlier candidate `a109e7c7488` had failed
only the unconditional schema-evolution identity assertion; the corrected requirement now passes.

Candidate `84f72b10c972490e4af78542197b1c579c9b5cca` also passed the complete gate in
[run 34276458479](https://github.com/unikdahal/spark/actions/runs/34276458479), including row-delete
refusal, shared-filesystem negative controls, child provenance and scratch isolation. The subsequent
canonical-manifest integration requires its own exact-candidate validation. No production authorization,
AQE recovery, performance or SPIP acceptance claim follows from the completed checks. Ordinary
latest-source resolution and ordinary errors remain authoritative.

## Validation workflow

Pushes to the owned branch run the dedicated workflow on the exact commit. The unrelated generic
Spark build matrix is skipped for this branch only. All builds and Spark tests run in GitHub Actions;
local checks are limited to source inspection, shell syntax and patch hygiene.

The final candidate gate requires the source conformance job as well as Core/SQL and quality lanes.
Core/SQL artifacts include exact suite reports and cold/healing child evidence. The shared-filesystem
experiment records the mount, process logs, result digests, skipped maps and provider reads.
It also requires distinct child process identities and matching commit provenance, and checks that
a changed source token or missing provider index triggers recomputation with identical results.

The next runtime work is to connect the certified actual SQL producer to publication and adoption,
then replace dense status reconstruction with explicit provider-native reads and prove complete
consumer invalidation. Those behaviors are not implemented merely because this document exists.
