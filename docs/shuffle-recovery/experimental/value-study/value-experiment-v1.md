# Prospective completed-shuffle reuse value experiment v1

Status: **preregistered, not executed**

This document freezes the first prospective value campaign for the experimental completed-shuffle
reuse prototype. It defines an explicit deployment scenario, exact cohort, failure distribution,
resource limits, statistical method, and economics rule before restart-speedup measurements are
observed.

This is an author-side experiment contract, not an Apache Spark or PMC acceptance requirement.
Changing a threshold, cohort member, weight, supported shape, timeout policy, exclusion rule, or cost
assumption after the campaign starts requires a new version. Historical failed or indeterminate
evidence remains published.

Companions:

- [`workload-scope-v1.md`](workload-scope-v1.md) freezes exact SQL, dataset provenance, producer and
  downstream scope, and primary map/reducer envelopes.
- [`value-experiment-v1.json`](value-experiment-v1.json) is the machine-readable campaign contract.

## Lineage and historical evidence boundary

- Frozen upstream baseline:
  `apache/spark@2a7cfea06ba135cf0ddc62902eb0daf5a835c672`.
- Frozen Phase 0 evidence:
  `7888d1e26d32e69694083e367ee6a92b28277c48`.
- Experimental branch base used for this preregistration:
  `5f773c4669d06af57216b09748ec7d168412ad3c`.
- The frozen Phase 0 result remains **MECHANISM_FEASIBLE / VALUE_GATE_NOT_MET**.
- The formal old value result remains **N/A** with 18 explicitly unweighted observations.
- The reported 36.8% failure-point opportunity is a projection, and the separate 19.4% eligible
  completed map-task-time share is not a measured restart speedup.

Nothing in this campaign relabels or replaces that result. The new campaign measures
failure-to-correct-result time on a later integrated real-source/real-provider candidate.

## Workload provenance and target deployment profile

Profile id: `decision-support-snapshot-rss-v1`.

The weights below are an **explicit scenario**, not production traces. No claim is made that they
represent a particular company or deployment. If real trace weights become available later, they
form a new prospective campaign instead of replacing these weights in place.

The first concrete target pairing is:

- immutable source: Apache Iceberg tables pinned to explicit snapshot ids;
- durable shuffle provider: Apache Celeborn as the first real remote-shuffle candidate;
- orchestrator: Kubernetes batch application relaunch with a fresh driver and fresh executors;
- driver: 4 vCPU, 16 GiB memory;
- executors: 32 pods, each 8 vCPU and 32 GiB memory;
- worker network assumption: 10 Gbit/s;
- fact input scales: 16, 64, 256 and 512 GiB of Iceberg data-file bytes, each +/-2%;
- nominal target fact snapshot: 256 GiB;
- dimension snapshot: 8 GiB +/-2%;
- retained completed-shuffle lifetime: 2 hours;
- scenario driver-failure frequency: 1.0% of query attempts.

Iceberg and Celeborn are targets, not presumed compatible implementations. If either cannot satisfy
the later source/provider gates, the outcome is a gate failure or a new prospective profile version;
this file is not silently retargeted.

The retry controller starts a replacement application after detecting producer-driver loss. The
replacement uses the same immutable source snapshot request and authenticated retry lineage, but it
receives new Spark application/attempt state, a fresh driver, fresh executors, and current shuffle
ids. Old numeric shuffle ids are never reused as identity.

## First reusable producer

Exactly one exchange may be adopted in a primary query.

The reusable producer grammar is frozen to:

```text
certified immutable snapshot scan
  -> optional reviewed deterministic filter
  -> optional reviewed deterministic projection
  -> hash partitioning OR single partitioning
  -> target exchange
```

The producer may not contain a join, aggregate, window, subquery, runtime filter, Python/UDF/native
expression, sampled range partitioning, or any runtime-dependent scan behavior.

Downstream consumers remain part of ordinary current Spark execution. The exact corpus deliberately
covers grouped aggregation, a partitioned window, a single-partition global aggregate, a downstream
sort-merge join, a later fresh shuffle, and sparse/empty-block behavior. Only the registered target
exchange may be adopted; later exchanges execute normally.

Excluded queries and behaviors are:

- writes and exactly-once commit recovery;
- streaming;
- incremental or externally delivered partial results;
- partial-map reuse;
- arbitrary RDD shuffle reuse;
- joins or aggregates below the reusable producer;
- unsupported expressions;
- runtime-filter-dependent scans;
- runtime-dependent source resolution;
- sampled range partitioning;
- cross-build reuse.

Any uncertainty is a cache miss. Ordinary Spark execution remains available.

## Frozen primary cohort

The exact SQL and structural admission rules are in `workload-scope-v1.md`.

| Id | Scenario weight | Target producer and downstream consumer |
| --- | ---: | --- |
| `G01` | 25% | scan/filter/project -> hash exchange -> grouped aggregate |
| `W01` | 15% | scan/filter/project -> hash exchange -> partitioned window |
| `S01` | 10% | scan/filter/project -> single-partition exchange -> global aggregate |
| `J01` | 20% | fact scan/filter/project -> hash exchange -> downstream sort-merge join and aggregate |
| `D01` | 20% | scan/filter/project -> hash exchange -> aggregate -> later fresh shuffle/consumer |
| `E01` | 10% | selective scan/filter/project -> hash exchange -> grouped aggregate, stressing empty blocks |

The weights sum to 100%. They are fixed scenario frequencies, not measurements from the frozen
Phase 0 corpus.

Default AQE is part of the headline campaign. The campaign does not change AQE, shuffle partition,
broadcast, or adaptive thresholds simply to manufacture a desired primary plan. AQE-disabled runs,
forced shuffle counts, broadcast controls, join hints, repartition hints, and similar plan-shaping
settings are **mechanism experiments only** and are excluded from value, overhead, and economics
headline gates.

## Supported map/reducer shapes

Primary mapper counts come from the resolved source decomposition. The accepted envelopes are frozen
before timing:

| Scale | Fact bytes | Accepted M | Hash R | Single R |
| --- | ---: | ---: | ---: | ---: |
| `I16` | 16 GiB +/-2% | 96-160 | 200 | 1 |
| `I64` | 64 GiB +/-2% | 384-640 | 200 | 1 |
| `I256` | 256 GiB +/-2% | 1,536-2,560 | 200 | 1 |
| `I512` | 512 GiB +/-2% | 3,072-5,120 | 200 | 1 |

The 200 hash reducers are the ordinary default initial shuffle partition count in the frozen Spark
lineage; the campaign does not set that value. AQE stays enabled and may coalesce or otherwise change
read partition specs after materialization. Those final specs are recorded evidence rather than
forced back to R.

A row whose source decomposition or initial reducer count falls outside its registered envelope is
`NOT_APPLICABLE`. Settings are not changed after observing performance to make it fit.

Mechanism-only forced probes may include 8,192 x 1,024 and 4,096 x 2,048. They never enter the
primary aggregate.

## Independent failure-free pilot and failure points

No restart-speedup pilot is used to choose the cohort or thresholds.

Failure-point structure is selected from the already frozen, independent, failure-free Phase 0-A
opportunity campaign:

- GitHub Actions run `33963836467`;
- artifact id `9969032965`;
- artifact digest
  `sha256:15bb5b3c2ebfbfa565c95163c9a2644cd9a147971f39c62d23583a84f6090013`.

That run predates this campaign and measured completed-shuffle opportunity rather than replacement
restart acceleration. Its only role here is to show that stage-relative early, middle, and late
failure landmarks are broadly applicable. Its performance percentages do not enter this campaign's
effect estimate, weights, confidence interval, or economics.

Conditional on a driver failure, the frozen failure distribution is:

| Id | Conditional weight | Injection rule |
| --- | ---: | --- |
| `F0` | 25% | producer computation completed but durable publication is not yet visible |
| `F1` | 30% | durable publication is visible and no material downstream post-exchange stage completed |
| `F2` | 30% | the first material downstream stage after the target exchange completed |
| `F3` | 15% | immediately before the final material downstream stage is launched |

`F0` is the required pre-publication negative control. Recovery must miss and fall back normally. A
pre-publication trial is never dropped because it cannot reuse the artifact.

A structural, failure-free admission pilot may verify exact source decomposition and stage
landmarks. It must not collect or use restart timing. If a query shape cannot expose a required
landmark, that cell is `NOT_APPLICABLE` before performance execution and its scenario weight is not
redistributed. More than 5% total scenario mass not applicable makes the campaign
**INDETERMINATE**.

## Timing boundary

Primary restart time is **failure-to-correct-result** wall-clock time.

The timer starts when the benchmark controller injects driver failure and ends only after the
replacement execution has produced the complete correct result and the controller has verified row
count and deterministic digest against its failure-free control result.

The timer includes, as applicable:

- failure detection;
- orchestration and retry delay;
- Kubernetes pod allocation;
- driver and executor startup;
- source resolution;
- planning and AQE planning;
- recovery eligibility/identity work;
- discovery and lookup;
- provider metadata paging;
- claim/binding;
- recovery reads;
- shuffle fetch;
- downstream execution;
- result verification.

No detection, allocation, planning, lookup, or read time is subtracted from the recovery arm.

## Trial count, pairing, order, cache warmth, and outliers

Each applicable `(cohort id, input scale, failure point)` cell uses **12 paired repetitions**. A pair
contains one ordinary-control attempt and one recovery-enabled attempt against the same source
snapshots, data scale, cluster shape, and failure landmark.

Pair order is block-randomized with seed `557003`, with six control-first and six recovery-first pairs
per complete cell. The exact generated order is retained in campaign artifacts.

The primary campaign uses natural backend cache warmth:

- no manual source-cache or provider-page-cache flush between paired attempts;
- randomized pair order prevents systematically assigning warmth to one arm;
- each failed producer and replacement uses fresh driver/executor processes;
- externally observable provider/source cache state is recorded;
- a separately labelled cold-provider-cache sensitivity run may be reported but cannot replace the
  primary result.

There is **no statistical outlier deletion, trimming, winsorization, or "obviously slow" rerun**. A
harness failure may invalidate a run only under a predeclared external-health rule, such as inability
to allocate the configured worker count before failure injection. The invalid raw record is retained,
and any replacement gets a new attempt id.

## Misses, failures, timeouts, and missing data

The hard per-attempt timeout is **1,800 seconds**.

Recovery misses are part of the recovery arm. A safe miss that recomputes normally is not missing
data and cannot be excluded from the speedup distribution.

Predetermined reporting rules are:

- recovery times out or fails to produce a correct result while control succeeds: assign recovery
  the 1,800-second timeout for the primary timing effect and record the reason;
- control times out or fails while recovery succeeds: give the pair no positive speedup credit,
  report it separately, and count it against the control reliability gate;
- both arms fail or time out: give the pair no speedup credit and count it against reliability;
- result digest or row-count mismatch: correctness failure; the campaign cannot PASS;
- provider/manifest uncertainty, timeout, malformed metadata, authorization failure, or ordinary
  recovery rejection: safe miss; retain the full observed recovery time;
- missing required timing or correctness evidence for a non-harness reason: campaign is
  INDETERMINATE unless one of the rules above classifies the raw failure.

The campaign cannot PASS if more than 1% of ordinary-control trials fail to reach a correct result,
or if any recovery trial produces a wrong result.

## Aggregation and confidence method

For each valid pair, let `T_control` and `T_recovery` be failure-to-correct-result time.

The headline aggregate improvement is:

```text
1 - weighted_sum(T_recovery) / weighted_sum(T_control)
```

Weights are the product of frozen cohort weight and conditional failure-point weight. Input scales
are reported individually and in an equal-scale aggregate; scale weights are not inferred from
observed performance.

The 95% confidence interval is a deterministic paired, stratified cluster bootstrap:

1. workload ids are top-level resampling clusters;
2. within each selected workload id, paired repetitions are resampled within failure-point and input
   scale strata;
3. frozen scenario weights are reapplied on every resample;
4. 10,000 bootstrap resamples are used;
5. bootstrap seed is `55003`;
6. physical exchanges are never treated as IID observations.

Raw paired observations, bootstrap input, seed, and rendered interval are retained as evidence.

## Prospective value and overhead gates

The author-side gates are frozen before execution.

### Restart benefit

PASS requires both:

- aggregate failure-to-correct-result improvement >= **20%**; and
- lower bound of the paired 95% confidence interval **> 0%**.

A positive point estimate with a non-positive lower bound does not pass. The threshold is not changed
after observing results.

### Disabled-path overhead

The recovery-disabled candidate is compared against the frozen upstream baseline build
`2a7cfea06ba135cf0ddc62902eb0daf5a835c672` on the same backend, source snapshot, JVM/compiler,
cluster shape, no-failure corpus, and randomized paired order.

The upper bound of the paired 95% confidence interval on runtime overhead must be <= **1%**.

### Enabled all-miss overhead

Recovery is enabled, but every lookup reaches a naturally absent or incompatible candidate through
predeclared identity/source conditions. No correctness validation is weakened and no provider
shortcut is added to manufacture a miss.

Relative to the same candidate with recovery disabled, the upper 95% confidence bound on no-failure
runtime overhead must be <= **5%**.

### Publication / no-failure overhead

The query successfully publishes a reusable completed exchange but no driver failure is injected.
Relative to the same candidate with recovery disabled, the upper 95% confidence bound on no-failure
runtime overhead must be <= **5%**.

All overhead campaigns use the same backend and primary corpus. A mechanism-only forced plan cannot
substitute for the default-AQE result.

## Resource and provider-index limits

The value campaign is valid only inside these prospective limits:

| Resource | Limit |
| --- | ---: |
| complete manifest bytes | 4 MiB |
| Spark driver recovery metadata for one adoption | 32 MiB |
| provider discovery payload per query | 8 MiB |
| examined candidate incarnations per exchange | 4 |
| provider index bytes charged to one retained artifact | 512 MiB |
| provider metadata page size | 1 MiB |
| provider metadata pages per adoption | 256 |
| recovery lookup deadline | 3 seconds |
| total pre-scheduler recovery preparation deadline | 10 seconds |
| concurrent provider reads per replacement query | 128 |
| retained shuffle artifact bytes per query | 256 GiB |
| retention | 2 hours |
| retained artifact byte-hours per query | 512 GiB-hours |

Provider-index bytes are measured and charged separately from shuffle payload bytes. A provider that
stores extra durable routing/index metadata must report those bytes; they cannot be hidden inside
"service overhead".

These are campaign limits, not a claim that the current implementation already enforces every
budget. A later implementation that cannot operate safely within them fails the registered profile
or requires a new prospective version.

## Cost model and deployment-economics gate

All economics are reported in **scenario USD** so compute, memory, durable storage, traffic, and
provider infrastructure share one unit. These rates are frozen planning assumptions, not vendor
quotes:

| Cost item | Scenario rate |
| --- | ---: |
| executor/driver vCPU | $0.040 per vCPU-hour |
| executor/driver memory | $0.005 per GiB-hour |
| durable retained storage | $0.000040 per GiB-hour |
| incremental provider read traffic/service | $0.010 per GiB |
| incremental provider write traffic/service | $0.010 per GiB |

For every query attempt, record:

```text
control compute cost
recovery compute cost
ordinary shuffle payload bytes written
incremental recovery-publication bytes written
provider-index bytes written
retained payload GiB-hours
retained index GiB-hours
incremental provider bytes read on recovery
```

Ordinary shuffle writes are reported but are **not double-counted** as a recovery feature cost when
an already deployed durable provider would have written those bytes anyway. Publication write cost
charges only bytes/service work introduced specifically to make the artifact reusable. If the
provider can retain already-written immutable blocks without copying, that incremental copy-byte
charge is zero and the retention/index/service cost remains visible.

Conditional-on-failure benefit is converted to expected per-query benefit using the frozen 1.0%
driver-failure frequency.

For an already deployed compatible durable provider:

```text
net_incremental_value =
  expected compute cost avoided
  - expected incremental recovery read cost
  - expected incremental publication write cost
  - retained payload storage cost
  - retained provider-index storage cost
  - measured incremental provider service cost
```

The deployment-economics gate requires `net_incremental_value > 0` and a lower 95% confidence bound
above zero under frozen scenario weights.

The cost of **introducing** durable shuffle storage is reported separately as
`greenfield_introduction_cost`: required provider nodes/disks, reserved capacity, control-plane
service cost, and amortized setup/operations cost in scenario USD. The report must show both:

1. economics when a compatible durable provider already exists; and
2. economics when durable storage must be introduced for this feature.

A positive existing-provider result may not be presented as positive greenfield economics.

## Change control and decision rules

Once any primary performance timing is observed, these cannot change in place:

- SQL corpus, source-scale admission rules, cohort weights;
- failure-point membership or weights;
- primary M/R envelopes;
- timeout and missing-data treatment;
- AQE/default-plan policy;
- cache-warmth and outlier policy;
- resource limits;
- confidence method or seeds;
- value/overhead/economics thresholds;
- scenario cost rates.

A material change creates `v2` or later and is prospective. The v1 raw data and decision remain
published even if a later design is better.

Decision values are:

- `PASS`: every correctness, restart-benefit, overhead, resource, and economics requirement passes;
- `FAIL`: a preregistered threshold is missed or a correctness/resource requirement is violated;
- `INDETERMINATE`: evidence quality, missing-data, reliability, or applicability rules prevent a
  valid decision.

No adverse result is converted into a prerequisite PASS.
