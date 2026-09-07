# Prospective completed-shuffle reuse value experiment v1

Status: **preregistered, not executed**

This document freezes the first prospective value campaign for the experimental completed-shuffle
reuse prototype. It defines an explicit deployment scenario, cohort, failure distribution,
resource limits, statistical method, and economics rule before restart-speedup measurements are
observed.

This is an author-side experiment contract, not an Apache Spark or PMC acceptance requirement.
Changing a threshold, cohort member, weight, supported shape, timeout policy, or exclusion rule after
the campaign starts requires a new version. Historical failed or indeterminate evidence remains
published.

The machine-readable companion is
[`value-experiment-v1.json`](value-experiment-v1.json).

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

- immutable source: an Apache Iceberg table pinned to an explicit snapshot id;
- durable shuffle provider: Apache Celeborn as the first real remote-shuffle candidate;
- orchestrator: Kubernetes batch application relaunch with a fresh driver and fresh executors;
- driver: 4 vCPU, 16 GiB memory;
- executors: 32 pods, each 8 vCPU and 32 GiB memory;
- worker network assumption: 10 Gbit/s;
- primary fact snapshot: 256 GiB compressed source data;
- current dimension snapshot for join consumers: 8 GiB;
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

Downstream consumers remain part of ordinary current Spark execution. The primary cohort deliberately
covers grouped aggregation, partitioned windows, a global ordered consumer, a downstream sort-merge
join, and a later fresh shuffle. Those consumers must be reviewed on the final Gate B candidate.
Only the target exchange is eligible for adoption; later exchanges execute normally.

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

All primary rows use Spark's default AQE setting for the tested build. The cohort is chosen by
operator shape, not by observed restart benefit.

| Id | Scenario weight | Target producer and downstream consumer |
| --- | ---: | --- |
| `G01` | 25% | scan/filter/project -> hash exchange -> grouped aggregate |
| `W01` | 15% | scan/filter/project -> hash exchange -> partitioned window |
| `S01` | 10% | scan/filter/project -> single-partition exchange -> global ordered consumer |
| `J01` | 20% | fact scan/filter/project -> hash exchange -> downstream sort-merge join with a current dimension scan |
| `D01` | 20% | scan/filter/project -> hash exchange -> aggregate -> later fresh shuffle/consumer |
| `E01` | 10% | highly selective scan/filter/project -> hash exchange -> grouped aggregate, including sparse or empty physical blocks |

The weights sum to 100%. They are fixed scenario frequencies, not measurements from the frozen
Phase 0 corpus.

Default AQE is part of the headline campaign. AQE-disabled repeats and any forced
`spark.sql.autoBroadcastJoinThreshold`, forced shuffle partition count beyond the frozen shape, or
similar plan-shaping setting are **mechanism experiments only** and are excluded from the value,
overhead, and economics headline gates.

## Supported map/reducer shapes

The primary supported shape set is frozen to:

| Maps | Reducers | Purpose |
| ---: | ---: | --- |
| 128 | 1 | single-partition / small shape |
| 512 | 64 | small distributed shape |
| 2,048 | 256 | target nominal shape |
| 4,096 | 512 | upper primary shape |

The following are labelled mechanism-only scaling probes and do not contribute to the primary value
estimate:

- 8,192 maps x 1,024 reducers;
- 4,096 maps x 2,048 reducers.

A campaign row outside these shapes is not silently added to the aggregate. Expanding the supported
shape set requires a new prospective version.

## Independent failure-free pilot and failure points

No new restart-speedup pilot is used to choose the cohort or thresholds.

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
| `F0` | 25% | producer computation has completed but durable publication is not yet visible |
| `F1` | 30% | durable publication is visible and no material downstream post-exchange stage has completed |
| `F2` | 30% | the first material downstream stage after the target exchange has completed |
| `F3` | 15% | immediately before the final material downstream stage is launched |

`F0` is the required pre-publication negative control. Recovery must miss and fall back normally.
A pre-publication trial is never dropped because it cannot reuse the artifact.

If a query shape does not expose a required stage-relative landmark, the trial is reported
`NOT_APPLICABLE` before execution and the scenario weight is not redistributed post hoc. A campaign
with more than 5% of total scenario mass not applicable is **INDETERMINATE**, not PASS.

## Timing boundary

Primary restart time is **failure-to-correct-result** wall-clock time.

The timer starts when the benchmark controller injects the driver failure and ends only when the
replacement execution has produced the complete correct result and the controller has verified its
row count and deterministic digest against the failure-free control result.

The timer therefore includes, as applicable:

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

No lookup, allocation, planning, or read time is subtracted from the recovery arm.

## Trial count, pairing, order, and cache policy

Each applicable `(cohort id, primary M/R shape, failure point)` cell uses **12 paired repetitions**.
A pair consists of one ordinary-control attempt and one recovery-enabled attempt against the same
source snapshot, data shape, cluster shape, and failure landmark.

Pair order is block-randomized with seed `557003`, with six control-first and six recovery-first
pairs per complete cell. The exact generated order is retained in campaign artifacts.

The primary campaign uses natural backend cache warmth:

- no manual source-cache or provider-page-cache flush between paired attempts;
- pair order is randomized to avoid assigning warmth systematically to one arm;
- each failed producer and replacement uses fresh driver/executor processes;
- provider and source cache state that is externally observable is recorded;
- a separate labelled cold-provider-cache sensitivity run may be reported, but it cannot replace the
  primary result.

There is **no statistical outlier deletion, trimming, winsorization, or "obviously slow" rerun**.
A harness failure may invalidate a run only under a predeclared external-health rule, such as the
cluster being unable to allocate the configured worker count before failure injection. The invalid
raw record is retained, and a replacement run gets a new attempt id.

## Misses, failures, timeouts, and missing data

The hard per-attempt timeout is **1,800 seconds**.

Recovery misses are part of the recovery arm. A safe miss that recomputes normally is not missing
data and cannot be excluded from the speedup distribution.

Predetermined reporting rules are:

- recovery times out or fails to produce a correct result while control succeeds:
  assign the recovery arm the 1,800-second timeout for the primary timing effect and record the
  concrete failure reason;
- control times out or fails while recovery succeeds:
  give the pair no positive speedup credit, report it separately, and count it against the control
  reliability gate;
- both arms fail or time out:
  give the pair no speedup credit and count it against the reliability gate;
- result digest or row-count mismatch:
  correctness failure; the campaign cannot PASS;
- provider/manifest uncertainty, timeout, malformed metadata, authorization failure, or ordinary
  recovery rejection:
  safe miss; retain the full observed recovery time;
- a trial missing required timing or correctness evidence for a non-harness reason:
  campaign is INDETERMINATE unless the raw failure can be classified under one of the rules above.

The campaign cannot PASS if more than 1% of ordinary-control trials fail to reach a correct result,
or if any recovery trial produces a wrong result.

## Aggregation and confidence method

For each valid pair, let `T_control` and `T_recovery` be failure-to-correct-result time.

The headline aggregate improvement is:

```text
1 - weighted_sum(T_recovery) / weighted_sum(T_control)
```

Weights are the product of the frozen cohort weight and conditional failure-point weight. Primary
M/R shapes are reported individually and in an equal-shape aggregate; shape weights are not inferred
from observed performance.

The 95% confidence interval is a deterministic paired, stratified cluster bootstrap:

1. workload ids are the top-level resampling clusters;
2. within each selected workload id, paired repetitions are resampled within failure-point and shape
   strata;
3. the frozen scenario weights are reapplied on every resample;
4. 10,000 bootstrap resamples are used;
5. bootstrap seed is `55003`;
6. physical exchanges are never treated as IID observations.

The raw paired observations, bootstrap input table, seed, and rendered interval are retained as
evidence artifacts.

## Prospective value and overhead gates

The author-side gates are frozen before execution.

### Restart benefit

PASS requires both:

- aggregate failure-to-correct-result improvement >= **20%**; and
- the lower bound of the paired 95% confidence interval is **> 0%**.

A positive point estimate with a non-positive lower bound is INDETERMINATE/FAIL for continuation;
the threshold is not changed after observing results.

### Disabled-path overhead

The recovery-disabled candidate is compared against the frozen upstream baseline build
`2a7cfea06ba135cf0ddc62902eb0daf5a835c672` on the same backend, source snapshot, JVM, compiler,
cluster shape, and no-failure query order.

The upper bound of the paired 95% confidence interval on runtime overhead must be <= **1%**.

This comparison is meaningful because the experimental lineage stays on the frozen upstream baseline;
the campaign must not silently rebase the candidate before this measurement.

### Enabled all-miss overhead

Recovery is enabled, but every lookup is forced to a naturally incompatible or absent candidate by
predeclared identity/source conditions. No validation rule is weakened and no artificial provider
shortcut is used.

Relative to the same candidate with recovery disabled, the upper 95% confidence bound on no-failure
runtime overhead must be <= **5%**.

### Publication / no-failure overhead

The query publishes a reusable completed exchange successfully, but no driver failure is injected.
Relative to the same candidate with recovery disabled, the upper 95% confidence bound on no-failure
runtime overhead must be <= **5%**.

All three overhead campaigns run on the same backend and primary cohort. A mechanism-only forced
plan does not substitute for the default-AQE result.

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
stores additional durable routing/index metadata must report those bytes; they cannot be hidden
inside "service overhead".

These are campaign limits, not a claim that the final implementation already enforces every budget.
A later implementation that cannot operate safely within them fails the registered profile or
requires a new prospective version.

## Cost model and deployment-economics gate

All economics are reported in **scenario USD** so compute, memory, durable storage, provider traffic,
and provider infrastructure use one unit. The following rates are frozen planning assumptions, not
vendor quotes:

| Cost item | Scenario rate |
| --- | ---: |
| executor/driver vCPU | $0.040 per vCPU-hour |
| executor/driver memory | $0.005 per GiB-hour |
| durable retained storage | $0.000040 per GiB-hour |
| provider read traffic/service | $0.010 per GiB |
| provider write traffic/service | $0.010 per GiB |

For every query attempt, record:

```text
control compute cost
recovery compute cost
shuffle payload bytes written
provider-index bytes written
retained payload GiB-hours
retained index GiB-hours
provider bytes read on recovery
provider bytes written on publication
```

Conditional-on-failure benefit is converted to expected per-query benefit using the frozen 1.0%
driver-failure frequency.

For an already deployed durable provider:

```text
net_incremental_value =
  expected compute cost avoided
  - expected recovery read cost
  - expected publication write cost
  - retained payload storage cost
  - retained provider-index storage cost
  - measured incremental provider service cost
```

The deployment-economics gate requires `net_incremental_value > 0` and a lower 95% confidence bound
above zero under the frozen scenario weights.

The cost of **introducing** durable shuffle storage is reported separately as
`greenfield_introduction_cost`: required provider nodes/disks, reserved capacity, control-plane
service cost, and amortized setup/operations cost in the same scenario USD unit. The report must show
both:

1. economics when a compatible durable provider already exists; and
2. economics when durable storage must be introduced for this feature.

A positive existing-provider result may not be presented as positive greenfield economics.

## Change control and decision rules

Once a performance campaign begins, the following cannot be changed in place:

- cohort membership or weights;
- failure-point membership or weights;
- primary M/R shapes;
- timeout and missing-data treatment;
- AQE/default-plan policy;
- cache-warmth policy;
- outlier policy;
- resource limits;
- confidence method or seeds;
- value/overhead/economics thresholds;
- scenario cost rates.

A material change creates `v2` or later and is prospective. The v1 raw data and decision remain
published even if v2 uses a better design.

The explicit decision values are:

- `PASS`: every correctness, restart-benefit, overhead, resource, and economics requirement passes;
- `FAIL`: a preregistered threshold is missed or a correctness/resource requirement is violated;
- `INDETERMINATE`: evidence quality, missing-data, reliability, or applicability rules prevent a
  valid decision.

No adverse result is converted into a prerequisite PASS.
