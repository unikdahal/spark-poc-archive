# Generic source and shuffle-provider integration

This is the implementation direction for the owned PoC, not a published Spark API or a claim that
all provider paths are implemented. Iceberg and Celeborn are the primary source and storage targets.
Core must work through capabilities and validated contracts, without branching on either name.

## Separate responsibilities

| Component | Responsibility | Must not decide |
| --- | --- | --- |
| Source connector | Certify the actual resolved read and ordered input decomposition. | Which old shuffle incarnation to adopt. |
| Spark SQL | Encode supported producer semantics, output format and partitioning. | Connector snapshot selection or provider retention. |
| Spark scheduler | Select accepted map winners, fence local decisions and invalidate consumers. | Provider file layout or connector-specific identity fields. |
| Shuffle provider | Seal, retain, claim and serve completed output with its own format and authorization. | Current source semantics or current SQL computation identity. |
| Recovery coordinator | Match full identities and orchestrate bounded asynchronous publication/claim. | Reinterpret unsupported reads or suppress ordinary source errors. |

## Source certificate

Certification observes the same resolved scan and ordered input partitions used by the current
query. It must not independently resolve a table name, substitute an older snapshot, or create a
second planning result. The certificate binds opaque versioned source facts to the exact mapper
decomposition. Spark adds the reviewed SQL producer, output and shuffle-partitioning semantics.
The connector binding must also identify the certification protocol and version, so unrelated
connectors cannot accidentally equate identical opaque bytes. A token is not a provider locator.

The implementation adds `ShuffleRecoverySourceToken.forProtocol` to frame a protocol ID and
opaque certificate with explicit lengths and an envelope marker. The protocol version remains in
the source token's version field. Protocol identifiers use strict UTF-8 with a 1 KiB encoded bound;
the complete frame, including metadata, must fit the existing 64 KiB source-token bound. The
factory defensively owns the certificate bytes. Protocol IDs are compared exactly; no case folding
or Unicode normalization is implied. The Range fixture now uses this factory. Legacy `copyOf`
callers retain their existing bytes, so adoption of the protocol factory is explicit rather than
an assertion that every existing caller already supplies namespaced facts.

This framing identifies a compatibility protocol; it does not authenticate a connector or establish
that its claims are true. A future source binding must associate the certificate with the actual
configured connector and planned scan. The framing patch and its regression tests still require
exact-candidate compilation and execution before a passing conformance result is claimed.

The connector either provides immutable bounded facts or explicitly refuses certification. Normal
source resolution and authorization happen first; their failures retain ordinary query semantics.
A certificate establishes read reproducibility, not authority to access source or retained output.
Unknown expressions, runtime-dependent planning and unsupported source features remain ineligible.

The current Iceberg spike demonstrates these facts using private connector hooks. The next source
integration must adapt those facts into a connector-neutral binding tied to the actual scan object.
The built-in Range adapter currently exercises canonical publication/recovery without such hooks;
it is a test fixture, not the connector contract itself.

## Provider contract

The configured provider supplies a versioned read-format compatibility ID. That ID enters the
computation identity before publication or lookup. A manifest cannot choose or load a provider
implementation; the configured implementation must recognize and validate any offered descriptor.
Matching a format ID alone does not authorize reads or establish descriptor validity.

Publication describes one completed shuffle with the scheduler-accepted map attempts. The provider
must durably seal that incarnation before Spark publishes it as reusable. A claim must establish
availability and retention for the replacement attempt before the scheduler suppresses map work.
The claim binds the recovery group, publishing incarnation, current attempt and current target.

The eventual read contract must address logical mapper/reducer ranges without requiring every
provider to expose local data/index files. A provider may use files, remote objects or its own block
protocol. Opaque provider descriptors remain bounded, versioned and validated by the selected
provider. Core retains only the information it needs for scheduling, empty ranges, resource bounds
and invalidation. The existing reference descriptor/read path is an initial format, not a required
layout for Celeborn or other implementations.

Remote publication, discovery and claim operations must stay off the DAGScheduler event loop.
Timeouts, cancellation and pre-adoption unavailability produce ordinary recomputation. Failure
after adoption requires fencing the binding and invalidating every dependent consumer before
recomputation, including any AQE or partially consumed state. Mixing outputs from two incarnations
must not be an implicit fallback.

## Implementation order and evidence

1. Carry canonical identities through publication, discovery and adoption. Exercise independent
   processes, changed SQL semantics, changed source facts and changed provider-format IDs.
2. Bind a connector certificate to its actual Spark batch scan. Connect Iceberg through that binding,
   with ordinary latest/snapshot/error behavior preserved.
3. Replace reference-specific retained-read assumptions with a provider contract. Integrate Celeborn
   against verified seal, claim, retention and read capabilities; do not infer them from API names.
4. Exercise the same contract tests with a second source/provider fixture to detect hidden coupling.
5. Prove late-failure invalidation and AQE behavior, then measure first-attempt cost and recovery value.

The first item passed for candidate `4aa9889f9350bad49f9b873d1ec9017a281c5cfa` in
[run 34316394901](https://github.com/unikdahal/spark/actions/runs/34316394901). The subsequent
provider-format input refinement requires its own exact-candidate validation. Items two through five
remain outstanding. The provider-selected format ID is an explicit SQL-builder input; the manifest
identity envelope preserves complete canonical bytes and supports opaque connector tokens. These
changes do not by themselves finish a generic public SPI or a Celeborn-backed recovery deployment.

## Iceberg planning constraint for the next adapter

The existing spike captures a planning event while forcing the query's actual scan planning.
Calling it after a caller has already materialized that plan may produce no event and must remain
unsupported. The integration must arrange the capture before planning, or obtain a certificate
retained by the connector alongside its planned tasks.

Reading a mutable table's current snapshot after planning is not an adequate replacement. The
[pinned SnapshotScan implementation](https://github.com/apache/iceberg/blob/e76d63584d7f83b102026749e1ae0f91813cb78e/core/src/main/java/org/apache/iceberg/SnapshotScan.java)
resolves an unspecified snapshot through the table's current metadata. That getter does not itself
prove which snapshot produced an already cached task list. The adapter must bind the certificate
to the read already planned, not perform another latest-snapshot resolution.

### Binding certificates to planned batch reads

The internal `ShuffleRecoverySourceBinding` adapter associates a protocol certificate and
ordered split descriptors with one `BatchScanExec` and its exact `InputPartition` objects.
The canonical builder admits that batch source only through this binding. A raw source token
alone does not certify a batch scan. Object identities are local validation facts and never
enter persisted identity bytes, so equivalent independently planned reads can still match.

The initial adapter refuses runtime filters, grouped partitions, empty reads, mismatched
partition objects, and oversized descriptors. It copies certificate and descriptor bytes.
Ordinary partition-planning exceptions propagate. A connector must still guarantee that
its certificate describes the actual planned read and that partition semantics remain
immutable: object identity cannot establish either guarantee on its own.

This is an internal single-source integration boundary, not a released connector API.
The out-of-tree Iceberg conformance adapter now feeds actual planned scans into this
binding and the canonical SQL builder. Automatic publication/recovery wiring and
Celeborn-native retention and reads remain outstanding. SQL canonical encoding advances to v4 for this
additional source operator; previous SQL identities conservatively miss.

The Iceberg bridge checks that each Spark input partition owns the exact cached Iceberg
task-group object certified in that ordinal. Its split descriptor includes the complete
ordered decomposition digest and ordinal. Conformance compares independent planning,
snapshot advancement, and a pinned older snapshot at a real hash-shuffle boundary, with
exact result checks. Merely implementing `HasPartitionKey` does not mean a read is grouped;
the binding rejects planned grouping through `BatchScanExec.keyGroupedPartitioning`.
This distinction matters because the pinned Iceberg partition implementation exposes that
interface even for ungrouped reads.

`ShuffleRecoveryCertifiedBatchInputs` is the connector-neutral handoff to the existing
publication and recovery identity inputs. It reads the exchange's semantic settings,
requires canonical admission, and checks the certified mapper count against the actual
shuffle dependency before returning `ShuffleRecoveryCanonicalInputs`. The selected provider
format remains an explicit argument. The Iceberg conformance bridge delegates to this
handoff. Ordinary reader-factory and dependency-planning failures propagate; this helper
does not publish artifacts, reserve adoption, or suppress ordinary query errors.

### Cold-process source adapters

The test-only `ShuffleRecoveryColdProcessSource` contract lets an external connector reuse
the existing producer, durable manifest, replacement-driver, and negative-control harness.
`SPARK_SHUFFLE_RECOVERY_TEST_SOURCE_ADAPTER` selects a class from the test classpath, loaded
independently in each child JVM. The adapter supplies session options, reconstructs the
query from durable source facts, and returns its certified identity inputs. Certificate
capture must happen during query planning, before the harness requests its exchange.
The returned query follows the harness's `id`, `k`, `payload` result contract.

The shared-filesystem proof exercises this entry point with the deterministic Range adapter,
including source-token, missing-artifact, and producer-filter controls. Existing cold-process
suites retain their original built-in source when the adapter setting is absent. An actual
Iceberg adapter must additionally capture the exact planned scan, persist source data across
children, and avoid reconstructing certificates from a later `latest` snapshot. The out-of-tree `ShuffleRecoveryIcebergColdSource` now implements this contract;
its cross-driver execution requires validation on the new candidate.

The dedicated Iceberg runner now invokes `cold-process.sh` after source conformance. Setup,
baseline, producer, replacement, and each negative control run in separate forked JVMs.
Only the warehouse and retained artifacts survive scratch cleanup. The runner requires
32 identical result rows, distinct process identities, a different replacement shuffle ID,
zero selected map tasks and positive provider reads on adoption, and recomputation without
provider reads for every negative control. One control overwrites the Iceberg table with
the same values and verifies that the committed snapshot ID changes before recovery.
This real snapshot change is separate from the synthetic source-token discriminator.

This new Iceberg proof uses local executors and the reference provider. It does not prove
Celeborn retention, remote Iceberg executors, production storage semantics, or automatic
query integration. CI preserves raw child logs, result evidence, and both snapshot IDs.
