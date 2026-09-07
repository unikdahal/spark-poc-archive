# Completed-shuffle value-study workload and scope matrix v1

This file freezes the executable workload membership and semantic scope used by
`value-experiment-v1.md`. It is prospective: no restart-speedup result has been observed for this
cohort.

The workload is an explicit deterministic scenario, not a production trace and not a claim about a
particular Spark deployment. The real-source campaign binds the logical tables below to explicit
Apache Iceberg snapshot ids. The exact snapshot ids, table metadata locations, file manifests, Spark
build SHA, provider build/version, cluster image digest, and source decomposition are recorded with
the later campaign artifacts; they are not invented in this preregistration.

## Dataset contract

The benchmark harness exposes two snapshot-pinned read-only relations to the SQL corpus:

```text
sr_fact(
  event_id BIGINT NOT NULL,
  tenant_id BIGINT NOT NULL,
  dimension_id BIGINT NOT NULL,
  day_id INT NOT NULL,
  sequence_id BIGINT NOT NULL,
  metric_value BIGINT NOT NULL,
  sparse_flag BOOLEAN NOT NULL
)

sr_dim(
  dimension_id BIGINT NOT NULL,
  region_id INT NOT NULL,
  segment_id INT NOT NULL
)
```

`sr_fact` is the only relation allowed below an adopted producer. `sr_dim` is used only by a
supported downstream consumer and is read normally by the replacement execution.

For reproducibility, synthetic row content is generated deterministically from ascending
`event_id`; no runtime clock, random source, user-defined function, or mutable external lookup is
used. The data generator version is `decision-support-generator-v1` and uses these value rules:

```text
tenant_id    = event_id mod 131072
dimension_id = event_id mod 1048576
day_id       = event_id mod 32
sequence_id  = event_id
metric_value = ((event_id * 17) mod 10000) + 1
sparse_flag  = (event_id mod 4096) = 0

sr_dim.dimension_id = 0 .. 1048575
sr_dim.region_id     = dimension_id mod 64
sr_dim.segment_id    = dimension_id mod 256
```

The generator materializes immutable Iceberg snapshots before any timed run. It may use generator
implementation details that are not themselves part of the query-under-test, but the resulting
snapshot ids and metadata/file manifests are fixed for every paired comparison.

The primary fact-input scale variants are 16 GiB, 64 GiB, 256 GiB, and 512 GiB of Iceberg data-file
bytes, each within +/-2% of the registered target. The dimension snapshot is 8 GiB +/-2%. The later
campaign records both table data-file bytes and actual bytes read by Spark so column pruning or
predicate effects are visible rather than inferred.

The nominal target deployment profile is the 256 GiB fact snapshot. The other registered input
sizes establish the supported scaling envelope without changing SQL text or shuffle settings.

## Default-plan rule

Primary evidence uses the tested Spark build's defaults for adaptive execution and shuffle
partition count. In particular, the campaign does not set `spark.sql.adaptive.enabled`,
`spark.sql.shuffle.partitions`, `spark.sql.autoBroadcastJoinThreshold`, or AQE broadcast/coalescing
thresholds merely to manufacture the requested plan.

For the frozen baseline lineage, the ordinary SQL shuffle partition default is 200 and AQE is on by
default. A primary row is accepted only if the actual physical plan still contains the intended
producer under those defaults. The exact initial reducer count, final AQE partition specs, query
stage plan, and adaptive-plan changes are captured for every run.

A forced shuffle count, disabled AQE, disabled broadcast, join hint, repartition hint, or similar
plan-shaping control is a separately labelled mechanism experiment and is excluded from headline
value, overhead, and economics gates.

## Supported mapper/reducer envelopes

Mapper decomposition comes from the certified resolved source, not from a benchmark-side
`repartition` call. The source certification step must report the ordered mapper decomposition
before a row is admitted to the timed campaign.

| Scale id | Fact data-file bytes | Accepted mapper count M | Hash producer R | Single producer R |
| --- | ---: | ---: | ---: | ---: |
| `I16` | 16 GiB +/-2% | 96-160 | 200 | 1 |
| `I64` | 64 GiB +/-2% | 384-640 | 200 | 1 |
| `I256` | 256 GiB +/-2% | 1,536-2,560 | 200 | 1 |
| `I512` | 512 GiB +/-2% | 3,072-5,120 | 200 | 1 |

The reducer counts above describe the target exchange's initial `ShuffleDependency`, not the number
of post-shuffle reader partitions after AQE coalescing. AQE remains enabled and its final partition
specs are evidence, not something this table forces to remain equal to R.

If a snapshot's default source planning falls outside its registered mapper envelope or a hash
exchange does not have the default 200 reducers, that `(workload, scale)` row is `NOT_APPLICABLE` for
primary evidence. The benchmark may not change source split or shuffle settings after observing
performance to force it into the envelope.

Mechanism-only probes may explicitly force alternative shapes, including 8,192 x 1,024 and
4,096 x 2,048. They are labelled and excluded from primary aggregate gates.

## Exact primary corpus

The six SQL templates below are the complete primary corpus. The logical view names are stable; the
campaign harness binds them to the exact registered snapshot-pinned relations before parsing the SQL.

### G01 - grouped aggregate consumer

Scenario weight: **25%**.

```sql
SELECT tenant_id, SUM(metric_value) AS metric_sum
FROM sr_fact
WHERE day_id BETWEEN 4 AND 27
GROUP BY tenant_id
```

Intended target producer:

```text
snapshot scan sr_fact
  -> filter day_id BETWEEN 4 AND 27
  -> project tenant_id, metric_value
  -> hash exchange on tenant_id
```

The grouped aggregate is downstream of the target exchange and executes under current Spark
semantics.

### W01 - partitioned window consumer

Scenario weight: **15%**.

```sql
SELECT
  tenant_id,
  event_id,
  ROW_NUMBER() OVER (
    PARTITION BY tenant_id
    ORDER BY sequence_id, event_id) AS rn
FROM sr_fact
WHERE day_id BETWEEN 4 AND 27
```

Intended target producer:

```text
snapshot scan sr_fact
  -> filter day_id BETWEEN 4 AND 27
  -> project tenant_id, event_id, sequence_id
  -> hash exchange on tenant_id
```

Window sorting and window evaluation are downstream. No Window operator is part of the reusable
producer.

### S01 - single-partition global aggregate consumer

Scenario weight: **10%**.

```sql
SELECT SUM(metric_value) AS metric_sum
FROM sr_fact
WHERE day_id BETWEEN 4 AND 27
```

Intended target producer:

```text
snapshot scan sr_fact
  -> filter day_id BETWEEN 4 AND 27
  -> project metric_value
  -> single-partition exchange
```

The final global aggregate is downstream. This row intentionally exercises the allowed
single-partition producer without relying on sampled range partitioning or a forced `ORDER BY` plan.

### J01 - downstream sort-merge join consumer

Scenario weight: **20%**.

```sql
SELECT
  f.tenant_id,
  d.region_id,
  SUM(f.metric_value) AS metric_sum
FROM sr_fact f
JOIN sr_dim d
  ON f.dimension_id = d.dimension_id
WHERE f.day_id BETWEEN 4 AND 27
GROUP BY f.tenant_id, d.region_id
```

The only adoption candidate is the fact-side producer:

```text
snapshot scan sr_fact
  -> filter day_id BETWEEN 4 AND 27
  -> project tenant_id, dimension_id, metric_value
  -> hash exchange on dimension_id
```

The dimension scan, its exchange if any, join, and final aggregation are ordinary downstream work.
The registered 8 GiB dimension snapshot is deliberately well above ordinary broadcast thresholds,
but the campaign does not disable broadcast or add a join hint. If the default/AQE plan nevertheless
removes the intended fact-side exchange, the row is `NOT_APPLICABLE`; it is not forced back into the
cohort.

### D01 - later fresh shuffle consumer

Scenario weight: **20%**.

```sql
WITH per_key AS (
  SELECT
    tenant_id,
    dimension_id,
    SUM(metric_value) AS metric_sum
  FROM sr_fact
  WHERE day_id BETWEEN 4 AND 27
  GROUP BY tenant_id, dimension_id
)
SELECT dimension_id, SUM(metric_sum) AS dimension_sum
FROM per_key
GROUP BY dimension_id
```

Intended target producer:

```text
snapshot scan sr_fact
  -> filter day_id BETWEEN 4 AND 27
  -> project tenant_id, dimension_id, metric_value
  -> hash exchange on tenant_id, dimension_id
```

The first aggregate and the later shuffle by `dimension_id` are downstream. Only the first exchange
may be adopted; the later exchange must execute fresh. This row checks that a supported query may
have additional downstream shuffle work without broadening the one-adoption contract.

### E01 - sparse/empty-block consumer

Scenario weight: **10%**.

```sql
SELECT tenant_id, SUM(metric_value) AS metric_sum
FROM sr_fact
WHERE sparse_flag = true
GROUP BY tenant_id
```

Intended target producer:

```text
snapshot scan sr_fact
  -> filter sparse_flag = true
  -> project tenant_id, metric_value
  -> hash exchange on tenant_id
```

The deterministic selectivity is intended to produce many empty mapper/reducer block combinations.
The query remains in the corpus even if a particular registered scale produces less sparsity than
expected; no result-driven replacement query is selected.

## Producer and downstream scope matrix

| Property | Producer allowed? | Downstream allowed in primary corpus? | Rule |
| --- | --- | --- | --- |
| explicit immutable Iceberg snapshot scan | yes | yes | current attempt must resolve and certify the requested snapshot |
| deterministic built-in projection | yes | yes | only expressions accepted by the closed identity allowlist |
| deterministic built-in filter | yes | yes | no runtime filter, subquery, or mutable lookup |
| hash partitioning | yes | n/a | initial reducer count must remain the default-plan value |
| single partitioning | yes | n/a | S01 only |
| grouped aggregate | no | yes | appears only after the target exchange |
| Window | no | yes | W01 only; sort/window are downstream |
| sort-merge join | no | yes | J01 only; fact target is before the join |
| later fresh shuffle | no | yes | D01 only; never adopted in the same query |
| broadcast conversion under default AQE | no producer change | conditionally | if it removes the registered target exchange, row is NOT_APPLICABLE |
| sampled range partitioning | no | no | mechanism-only if separately studied |
| runtime-filter/DPP-dependent scan | no | no | excluded from v1 |
| Python/UDF/native expression | no | no | excluded from v1 |
| subquery below producer | no | no | excluded from v1 |
| write/streaming operation | no | no | excluded from v1 |
| partial-map adoption | no | no | complete exchange only |
| second adopted exchange | no | no | at most one adoption per query |

## Corpus immutability and admission

Before the first timed value observation, the campaign artifact must contain for every primary
`(workload id, scale id)` row:

- SQL text digest matching this file;
- Spark exact head SHA and build information;
- source adapter/version and explicit Iceberg snapshot id;
- source schema/field ids and ordered mapper decomposition certification;
- data-file byte total and actual planned input-partition count;
- physical-plan digest and the exact target exchange ordinal/identity;
- initial target mapper and reducer counts;
- AQE enabled state and relevant default configuration snapshot;
- provider build/version and storage namespace;
- whether the row is `ADMITTED` or `NOT_APPLICABLE`, with a pre-performance structural reason.

Admission is based only on semantic/structural compatibility and the registered M/R envelope. It may
be performed in a failure-free structural pilot. Restart timing from that pilot is neither collected
nor used for cohort selection.

After any primary restart or overhead timing is observed, corpus admission is closed. A structural
change to SQL, source snapshots, scale envelopes, scenario weights, or plan-acceptance rules requires
a new prospective experiment version.
