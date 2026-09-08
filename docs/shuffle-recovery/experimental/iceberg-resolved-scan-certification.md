# Iceberg resolved-scan certification spike

## Decision

Apache Iceberg 1.11.0 can provide enough information to certify a deliberately narrow Spark batch
snapshot scan, but the required information is not exposed through a stable connector-facing API.
The feasibility adapter therefore remains out of tree with respect to Spark's normal source roots and
uses Iceberg package-private scan classes from the pinned release. This is a positive feasibility
result for the information content and a negative result for treating the current access path as a
stable Spark source contract.

The prototype should require a future adapter boundary that receives a certificate from the exact
resolved source scan. It should not reproduce the spike's package-private dependency or independently
reload a table to discover a snapshot.

## Pinned source

The spike pins:

- Apache Iceberg `1.11.0`;
- `iceberg-spark-runtime-4.0_2.13`;
- Iceberg source tag `apache-iceberg-1.11.0` for the API review.

The dependency is downloaded only by `dev/shuffle-recovery/iceberg-source-spike/run.sh`. It is not a
Spark module dependency and is not added to Spark's runtime dependency graph.

## Why the certificate is derived from the executed scan

Spark's `BatchScanExec` owns the resolved Data Source V2 `Scan`. For Iceberg 1.11.0 the concrete
`SparkBatchQueryScan` caches the file tasks and task groups that it plans. `SparkBatch.toBatch()` then
passes those same task groups to `SparkBatch.planInputPartitions()`, where each task group becomes one
Spark input partition.

The spike enters through that `BatchScanExec.scan` object and forces/reads the scan's cached task
groups. It never loads the table again to decide which snapshot or files should be read.

There is one important latest-snapshot race to avoid. Iceberg's `SnapshotScan.planFiles()` chooses the
snapshot and emits a `ScanEvent` before planning files. Calling `SnapshotScan.snapshot()` after
planning is not sufficient for an unpinned scan because it may observe a newer
`table.currentSnapshot()`. The spike captures the synchronous `ScanEvent` on the planning thread and
binds its snapshot ID, projection and pushed filter to the same planning operation that produced the
cached task groups. A separately re-resolved table lookup is intentionally not used.

Iceberg's listener registry has no unregister operation. The spike registers one bounded process-life
listener and uses a `ThreadLocal` capture that is always removed in `finally`. That mechanism is
acceptable only for this isolated conformance process. It is specifically **not** the recommended
production extension point.

## Certified scan facts

For the supported slice the bounded identity contains:

- Iceberg table UUID from the resolved scan's table object;
- the exact snapshot ID emitted by the same `planFiles()` operation;
- projected Iceberg schema, including field IDs and types;
- the Iceberg pushed filter expression including literal values;
- case-sensitivity policy;
- effective split size, split lookback and open-file cost used by the underlying scan;
- mapper/task/delete counts; and
- a SHA-256 mapper-decomposition certificate.

The mapper-decomposition hash is built in input-partition order. Within each mapper it preserves file
task order and covers:

- data-file content kind, location, format and partition-spec ID;
- record count and physical file size;
- data/file sequence numbers, first row ID and sort-order ID when present;
- task byte start and length;
- the task residual expression;
- delete-file count and order; and
- for every delete file, content kind, location, format, spec ID, record/file sizes, sequence numbers,
  equality field IDs, referenced data file and deletion-vector offset/length when present.

The supported spike is intentionally restricted to unpartitioned `FileScanTask` scans with no Spark
runtime filters and no Iceberg grouping key. Partition transforms, runtime-filter replanning,
incremental/changelog scans, metadata tables, aggregate/local scans and other unreviewed source
features are recovery misses.

## Boundedness and trust boundary

The adapter treats source planning state as untrusted for recovery purposes. It refuses certification
when any configured bound is exceeded rather than allocating a representation proportional to an
unbounded source response. The spike limits task groups, file tasks, delete files per task, individual
strings, total metadata fed into the decomposition digest and final identity bytes. The durable
certificate is O(1) in mapper/file metadata size: task detail is streamed into a fixed-size digest and
is not copied into a dense mapper-by-reducer structure.

Raw file locations are not printed into the evidence artifact. They contribute only to the
decomposition digest.

## Ordinary query semantics

Recovery certification is an optional observation after ordinary source resolution. The adapter does
not catch source planning exceptions and convert them into misses. This distinction is required:

- **certificate unavailable after a valid scan resolves**: return an unsupported/miss result and let
  the ordinary query execute unchanged;
- **source resolution, snapshot selection or authorization fails**: propagate the ordinary Spark /
  source error. Recovery does not substitute a historical snapshot and does not turn the error into a
  cache miss followed by different source semantics.

The conformance program checks an unavailable/expired explicit snapshot and a missing source through
ordinary planning. A deployment-specific authorization backend is not introduced by this spike; the
same structural rule applies because certification is never allowed to authorize or re-resolve a
query.

## Supported-feature matrix

| Feature | Spike disposition | Reason |
| --- | --- | --- |
| Unpartitioned batch table scan | Certified | Exact cached task groups map to Spark input partitions. |
| Mutable latest | Certified | Snapshot ID comes from the same `planFiles()` event; a later snapshot produces a different identity. |
| Explicit snapshot ID | Certified while retained | The requested snapshot is resolved by ordinary Iceberg planning; after expiry the ordinary error is preserved. |
| Projection / field IDs | Certified | Iceberg projected schema is encoded, including field IDs. |
| Pushed filter | Certified | Iceberg expression JSON with literal values is encoded. |
| Per-file residual | Certified | Each `FileScanTask.residual()` is part of mapper decomposition. |
| Split planning | Certified | Effective split controls plus exact ordered task groups are covered. |
| Delete files attached to file tasks | Descriptor supported | Delete content/sequence/equality/DV facts are hashed; no claim is made for unreviewed row-level scan modes outside the supported batch slice. |
| Schema evolution outside the projection | Allowed when resolved projected schema/decomposition is unchanged | Recovery identity follows the scan used for execution, not unrelated table metadata. |
| Schema evolution affecting projected fields | Miss | Projected field IDs/types change. |
| Partitioned/grouped scans | Unsupported | Partition/group-key canonicalization has not been reviewed for this spike. |
| Runtime-filter-dependent scans | Unsupported | Runtime filters may reset Iceberg tasks after initial planning. |
| Incremental/changelog/streaming scans | Unsupported | Outside the batch snapshot scope. |
| Aggregate/local metadata scans | Unsupported | They do not use the reviewed `SparkBatchQueryScan` path. |
| Deployment-specific authorization | Ordinary source responsibility | Certification cannot grant access or replace a denied query with historical data. |

## Deterministic conformance cases

`dev/shuffle-recovery/iceberg-source-spike/run.sh <evidence.tsv>` compiles the external adapter only for
the conformance invocation and runs the following relationships against ordinary Spark execution:

1. the initial latest scan certifies its actual current snapshot and returns the expected rows;
2. after an append advances latest, the new latest scan uses the new snapshot and has a different
   identity;
3. an explicit read of the retained old snapshot reproduces the original resolved-scan identity and
   old rows;
4. changing a pushed filter changes identity while ordinary filtered results remain correct;
5. changing split planning changes the ordered mapper-decomposition certificate without changing
   rows;
6. schema evolution is compared at the projected-field boundary;
7. an unsupported partitioned scan returns no recovery certificate but executes normally;
8. after the old snapshot is expired, an explicit request for it preserves Iceberg's ordinary
   planning error; and
9. a missing source likewise fails in ordinary source resolution rather than becoming a recovery
   decision.

The runner writes the selected table UUID, concrete snapshot IDs, mapper/file/delete counts and PASS
markers to its evidence file. CI uploads that file from the exact pull-request head.

## Gap to a stable adapter

The information required for a correct certificate exists in Iceberg 1.11.0, but assembling it today
requires connector-private access plus a process-global planning listener. Neither should become a
Spark API dependency. A production-quality source contract should instead let the source return a
bounded immutable certificate from the same resolved scan object after all decomposition-affecting
planning is fixed, together with an explicit unsupported result. It must preserve ordinary source and
authorization errors separately from certificate unavailability.

This spike therefore supports continuing the prototype with a narrow external Iceberg adapter, while
recording a concrete API gap for any later stable design.
