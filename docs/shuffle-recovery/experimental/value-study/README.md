# Completed-shuffle recovery prospective value study

Status: **preregistered only; no v1 performance campaign has been executed**

This directory freezes the author-side value-study contract for the experimental completed-shuffle
recovery program. It does not change Spark runtime behavior and does not relabel the frozen Phase 0
`MECHANISM_FEASIBLE / VALUE_GATE_NOT_MET` result.

## Registered package

- [`value-experiment-v1.md`](value-experiment-v1.md): deployment scenario, failure distribution,
  timing boundary, run counts, missing-data rules, confidence method, gates, budgets, and economics
  decision rules.
- [`workload-scope-v1.md`](workload-scope-v1.md): deterministic dataset provenance, exact six-query
  corpus, supported producer grammar, downstream-consumer review, default-AQE policy, and primary
  mapper/reducer envelopes.
- [`value-experiment-v1.json`](value-experiment-v1.json): machine-readable frozen experiment fields
  used to detect later cohort/threshold drift.
- [`cost-model-v1.md`](cost-model-v1.md): common scenario-USD rates plus separate numerical
  existing-provider and greenfield durable-storage accounting.

## What is frozen before performance observation

The package fixes:

- an explicit scenario rather than pretending to have production trace weights;
- Iceberg explicit-snapshot reads and Celeborn as the first real source/provider targets, without
  presuming either later feasibility gate passes;
- one adopted exchange whose producer is a certified scan plus optional deterministic filter/project
  followed by hash or single partitioning;
- exact SQL corpus membership and downstream consumers;
- default AQE/default shuffle/broadcast behavior for primary evidence;
- source-size and mapper/reducer envelopes;
- independent failure-free structural admission and four weighted failure landmarks, including the
  mandatory pre-publication miss;
- 12 randomized control/recovery pairs per applicable workload/scale/failure cell;
- failure-to-correct-result timing including detection, orchestration, allocation, planning, lookup,
  provider metadata, recovery reads, downstream execution, and result verification;
- timeout, recovery-miss, failed-trial, missing-data, cache-warmth, and no-outlier-deletion rules;
- paired clustered-bootstrap confidence intervals;
- >=20% restart improvement with lower 95% confidence bound above zero;
- disabled <=1%, enabled-all-miss <=5%, and publication/no-failure <=5% upper-95% overhead gates;
- manifest/driver/provider-index/read/deadline/artifact/byte-hour limits;
- common-unit economics, including a separate numerical greenfield introduction scenario.

Any material change after primary timing starts requires a new prospective version. v1 remains
published even if it fails or is indeterminate.

## Current implementation boundary

The current experimental Spark tree still uses the narrow historical/reference recovery mechanisms
and does not yet contain the real Iceberg certification or Celeborn cold-driver capability required
to execute this campaign. Those missing capabilities are deliberately not scaffolded here.

A later campaign may admit a row only after the real source/provider/integration work proves the
registered physical and semantic boundary. Inability to do so is a gate result, not permission to
weaken the cohort after performance outcomes are visible.

This preserves recovery as an optional cache: uncertainty or incompatibility produces ordinary Spark
execution, and no value-study setting weakens correctness validation.
