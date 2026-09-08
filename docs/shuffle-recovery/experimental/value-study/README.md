# Completed-shuffle recovery prospective value study

Status: **v2 preregistered only; no primary restart or overhead campaign has been executed**

This directory freezes the author-side value-study contract for the experimental completed-shuffle
recovery program. It does not change Spark runtime behavior and does not relabel the frozen Phase 0
`MECHANISM_FEASIBLE / VALUE_GATE_NOT_MET` result.

## Active prospective package

- [`value-experiment-v2.md`](value-experiment-v2.md): active deployment-value contract, including
  recovery-independent failure selection, matched disabled controls, primary accounting, run counts,
  overhead cells, confidence methods, gates, and change control.
- [`value-experiment-v2.json`](value-experiment-v2.json): machine-readable active contract.
- [`workload-scope-v1.md`](workload-scope-v1.md): unchanged deterministic dataset provenance, exact
  six-query corpus, supported producer grammar, downstream-consumer review, default-AQE policy, and
  primary mapper/reducer envelopes.
- [`cost-model-v1.md`](cost-model-v1.md): unchanged common scenario-USD rates plus separate numerical
  existing-provider and greenfield durable-storage accounting.

## Preserved preregistration history

[`value-experiment-v1.md`](value-experiment-v1.md) and
[`value-experiment-v1.json`](value-experiment-v1.json) remain published unchanged. v1 was superseded
prospectively because its primary F0/F1 failure landmarks depended on recovery publication and did not
define the same intervention for the recovery-disabled control.

Before v2 was registered:

- no primary restart timing had been observed;
- no primary overhead timing had been observed;
- no real Iceberg/Celeborn value campaign had been executed;
- no new benchmark was run to choose the v2 correction.

Historical Phase 0 opportunity evidence remains historical context only. It is not used to select v2
failure offsets or to estimate restart benefit.

## What v2 freezes before performance observation

The active package fixes:

- the same explicit deployment scenario, workload cohort, producer grammar, default-AQE policy,
  input scales, weights, resource limits, thresholds, and economics assumptions registered in v1;
- a mandatory independent failure-free calibration on the integrated candidate with recovery
  disabled: 24 workload/scale cells x 5 ordinary runs = 120 no-failure attempts;
- four primary failure offsets per workload/scale derived only from ordinary Spark progress elapsed
  times, then frozen in `primary-failure-schedule-v2.json` before any restart timing begins;
- the exact same frozen elapsed-time offset in the recovery-disabled and recovery-enabled arms;
- no primary wait for publication, provider visibility, reuse success, or a recovery-only event;
- after-the-fact publication-state accounting so late/failed/unavailable publication remains an
  observed miss/outcome instead of disappearing from the cohort;
- explicit retention of failures before producer completion and zero-impact cases where a correct
  result completes before the scheduled failure, with no post-hoc weight redistribution;
- optional publication-boundary mechanism probes with zero primary scenario weight;
- 96 restart cells, 12 randomized pairs per cell, 1,152 pairs / 2,304 timed restart attempts;
- three separately registered no-failure overhead comparisons, each with 24 cells and 12 pairs per
  cell, totaling 72 cells / 864 pairs / 1,728 timed overhead attempts;
- comparison-specific overhead randomization and paired-bootstrap seeds;
- failure-to-correct-result residual accounting, conservative timeout scoring, no outlier deletion,
  and fixed scenario composition;
- >=20% restart improvement with lower 95% confidence bound above zero;
- disabled <=1%, enabled-all-miss <=5%, and publication/no-failure <=5% upper-95% overhead gates;
- common-unit existing-provider and greenfield economics without survivor-only failure weighting.

The numeric 24-row failure schedule is intentionally not fabricated in this preregistration change.
It must be produced from the later integrated real deployment's independent ordinary dry runs and
frozen before primary timing. If those runs cannot produce the required schedule, the primary
campaign does not start under v2.

## Current implementation boundary

The experimental Spark tree is still being assembled toward the real source/provider candidate
required by the campaign. This package does not scaffold Iceberg certification, Celeborn discovery,
authenticated lifecycle, publication, adoption, AQE recovery state, or benchmark execution.

A later campaign may run only after the real integration proves the registered physical and semantic
boundary and emits the required failure schedule. Inability to do so is a gate result, not permission
to weaken the cohort after performance outcomes are visible.

This preserves recovery as an optional cache: uncertainty or incompatibility produces ordinary Spark
execution, publication failure remains best-effort, and no value-study setting weakens correctness
validation.
