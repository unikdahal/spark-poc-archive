# Owned completed-shuffle PoC

The active integration candidate is `spip/poc-owned-end-to-end`. Work is driven by executable
behavior and evidence, not by the old issue boundaries. The frozen Phase 0 evidence remains
unchanged. Open-PR source compatibility and evidence-validation work is preserved in merge history.

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

Candidate `a109e7c74882089dacf6fd9d02d19b7b15a8a2f1`, Actions run `34270239031`, passed
routing, lint/license, Core, SQL and cold-process validation. Its Iceberg compatibility smoke passed,
but source conformance failed on an unjustified unconditional schema-evolution identity assertion.
The next candidate records stable identity or a conservative miss and still requires exact values.
The downloaded SQL artifact validates 39 cold child records, including nine successful replacements
and ten negative controls, plus three healing child records against that exact candidate.

No performance, production authorization, AQE recovery or SPIP acceptance claim follows from these
checks. Ordinary latest-source resolution and ordinary errors remain authoritative.

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
