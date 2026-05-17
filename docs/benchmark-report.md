# Parameter Sensitivity in Streaming Change-Point Detectors

## Setup

Synthetic signals with a known change point at sample 100. Baseline: N(50, σ²), post-change: N(50 + δ, σ²). Noise levels σ = 2, 5, 10. Shift sizes 1σ, 2σ, 5σ. 1000 trials per configuration, fixed seed (42).

Two parameter sets: production defaults (CUSUM k=0.5/h=4.0, Kalman measurement_noise=5.0) and tuned (CUSUM k=1.0/h=8.0, Kalman measurement_noise=50.0). Tuned parameters were selected against the same noise/shift grid used for evaluation. A held-out split would give a more conservative estimate of tuned performance.

Bayesian Online CPD was implemented but excluded - see bottom.

## Findings

**Default CUSUM:** 100% detection across all configurations. ~66% false positive rate - the CUSUM statistic random-walks past h=4.0 over 200 samples roughly two-thirds of the time. The 100% detection rate is partly inflated - since alerts are only checked after the change point, some may have triggered earlier on noise alone. The 66% FPR on stable signals supports this.

**Tuned CUSUM:** 0% FPR across all configurations. 99%+ detection for shifts ≥2σ. 1σ shifts caught ~4% of the time. Detection delay consistent across noise levels: median 3 samples at 5σ, 11 at 2σ. Noise-invariant because CUSUM operates on z-scores.

| Noise | Shift | Detection Rate | Median Delay | FPR |
|-------|-------|---------------|-------------|-----|
| 2.0 | 2.0σ | 99.3% | 11 | 0.0% |
| 2.0 | 5.0σ | 100.0% | 3 | 0.0% |
| 5.0 | 2.0σ | 99.8% | 11 | 0.0% |
| 5.0 | 5.0σ | 100.0% | 3 | 0.0% |
| 10.0 | 2.0σ | 99.4% | 11 | 0.0% |
| 10.0 | 5.0σ | 100.0% | 3 | 0.0% |

**Default Kalman:** Works at σ=2 (3-4% FPR). At σ≥5, measurement_noise=5.0 is too small - 100% FPR.

**Tuned Kalman:** Works at σ=5 only for large shifts (89.9% at 5σ, 0.8% FPR; drops to 0.2-1.6% at smaller shifts). At σ=2, measurement_noise=50.0 is too wide - nothing fires. At σ=10, fires on everything (99%+ FPR alongside 96%+ detection) - same always-fires pathology as default at σ≥5. Fixed measurement noise cannot span multiple noise regimes. The residual threshold only works when the configured noise matches the actual noise. When they don't match, the threshold either always fires or never fires.

## Problems

- CUSUM parameter tuning matters more than algorithm choice. Default vs tuned is the difference between 66% FPR and 0%.
- Kalman requires noise model calibration matched to the actual signal. No single parameter set works across regimes. Adaptive measurement noise estimation would fix this.
- Bayesian CPD did not fire in any of 18,000 trials, including stationary-signal trials where some baseline-rate firing would be expected. This is a deterministic failure mode (zero firings, not low rate), suggesting a bug rather than miscalibration. The implementation is in the codebase pending investigation.

## Reproducing

```text
java -cp target/observability-engine-1.0.0.jar com.engine.benchmark.BenchmarkRunner
```

Raw data: `docs/benchmark-results.csv`
