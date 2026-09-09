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

The first item is now implemented and under exact-candidate CI validation. Items two through five
remain outstanding. The provider-selected format ID is an explicit SQL-builder input; the manifest
identity envelope preserves complete canonical bytes and supports opaque connector tokens. These
changes do not by themselves finish a generic public SPI or a Celeborn-backed recovery deployment.
