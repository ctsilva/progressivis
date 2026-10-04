# PLAN — Parallel ProgressiVis

Working tracker for the `parallel-scheduler` branch. Rationale, measurements and
full design: [`design/parallel_progressivis.md`](design/parallel_progressivis.md).

Legend: `[x]` done · `[~]` in progress · `[ ]` to do

## Environments

- `.venv` — CPython 3.14 (GIL): `uv venv --python 3.14 .venv && uv pip install --python .venv -e '.[test]'`
- `.venv-ft` — CPython 3.14t (free-threaded): same with `--python 3.14t .venv-ft`.
  Run with `PYTHON_GIL=0` until datasketches supports free-threading.

## Phase 0 — single-threaded fixes and benchmark

- [x] Design note committed (`design/parallel_progressivis.md`)
- [x] Add `PIntSet.__array__` (via `BitMap.to_array()`, int64) — 300k-row read 12.4 ms → 0.30 ms
- [x] Read path: convert contiguous `PIntSet` to slice before indexing storage
      — hot site: `table/column_selected.py:49` → `PColumn.__getitem__` (`table/column.py:284`);
      also `PColumn.read_direct`. Result is still a copy (same semantics as before).
- [ ] Later: return zero-copy views for contiguous reads (needs audit of callers that mutate results)
- [ ] Later: `PColumn.__getitem__` turns ndarray indices into Python lists (`column.py:286`) — slow, check why
- [x] Full test suite: no new failures. 518 passed, 29 failed, 37 skipped (18 min, 1M rows);
      all 29 also fail on unchanged code (see "Known failures on master" below)
- [x] Add `scripts/bench_parallel.py` (serial vs threaded, wall/CPU, GIL status)
- [x] Record before/after numbers below
- [ ] Investigate ~20% scheduler idle time (CPU/wall ≈ 0.8)

## Test suite: never hang, run fast

- [x] Per-test timeout: `pytest-timeout` in `test` extras, `timeout = 600` in `pyproject.toml`
- [x] `tests/conftest.py`: on timeout, report lists each module's state and its input slots
- [x] `bigfile` size configurable: `progressivis.datasets.bigfile_rows()`,
      env `PROGRESSIVIS_BIGFILE_ROWS`; tests default to 100k rows (`tests/__init__.py`);
      non-default sizes get their own file name (`bigfile_100000.csv`)
- [ ] Fix the datetime slow path that looks like a hang: `PTable.append` (`table/table.py:397-415`)
      falls back to a per-row Python loop when a datetime column fails the vectorized copy
      (`ValueError: could not broadcast (1000,) into (1000,6)`). Hits `test_03_join::test_outer_pu`
      and `test_03_threaded_csv::test_read_csv_taxis` (taxi data): each > 5 min, 600 s of the 18-min run.
      Passes in CI, so probably triggered by pandas 3 / pyarrow 25. One `run_step` far exceeds its quantum.
- [ ] Offline-safe tests: skip (not fail) when a download is unavailable (PPCA, taxis, `test_as_array3`)
- [ ] HTTP tests (`test_03_csv_over_http`): local RangeHTTPServer often refused connections here; flaky

### Known failures on master (macOS, Python 3.14, pandas 3.0.6, pyarrow 25)

- `test_00_storageengine::test_storage_engines` — mmap temp dir unset (order-dependent)
- `test_03_aggr` (2) — test asks for `"uniq"`, aggregator registered as `"nunique"` (`stats/online.py:113`)
- `test_03_groupby` (5), `test_03_recoverable_csv` (8) — fail identically on master; not investigated
- `test_03_csv_over_http` (3–6) — local HTTP server connection refused
- `test_03_ppca` (7), `test_03_csv::test_as_array3`, `test_03_threaded_csv::test_read_csv_taxis` — downloads 404 / time out
- `test_03_join::test_outer_pu` — datetime slow path (above)

## Phase 1 — task parallelism ("wave" scheduler)

- [ ] Thread-safety audit: name generation, `StorageManager`, tracers, `np.random` in `RandomPTable`, KLL/datasketches
- [ ] Wave selection: ready modules, no two directly connected
- [ ] Serial `prepare_run`, parallel `run_step` on a thread pool, serial `after_run`
- [ ] `Scheduler(parallel=..., max_workers=...)`, opt-out attribute for non-thread-safe modules
- [ ] Interaction: `for_input()` limits wave eligibility / reserves cores
- [ ] Control BLAS/numpy threads (oversubscription)
- [ ] Test suite passes in parallel mode
- [ ] CI job on 3.14t; fail if GIL is re-enabled at import
- [ ] Measure on a ≥ 32-core machine (taxi heatmap, scaler demo, PPCA)

## Phase 2 — data parallelism inside modules

- [ ] Split step rows across workers + merge: Min/Max, Var, Histogram1D/2D, KLL, Stats
- [ ] Time predictor aware of worker count

## Phase 3 — pipeline parallelism

- [ ] Chunked append-only column storage (no `np.resize` reallocation under readers)
- [ ] Allow consumer to run concurrently with producer on committed rows

## Phase 4 — later

- [ ] Multiple implementations per module (CPU/GPU, HyperFlow-style)
- [ ] Native core for change management, only if profiling justifies

## Measurements

4 independent pipelines (`RandomPTable` 1M×10 → Min, Max → Histogram2D; Var), 4 P-cores, Apple Silicon.
Run: `python scripts/bench_parallel.py 4 1000000` (add `PYTHON_GIL=0` **only** on the 3.14t build —
on a standard build it is a fatal startup error). Single runs vary a lot when other apps are busy; use the median of ≥ 3.

| Date | Build | Code | Serial | Threaded | Speedup |
|---|---|---|---|---|---|
| 2026-10-04 | 3.14 | baseline | 33.6 s | 36.6 s | 0.92x |
| 2026-10-04 | 3.14t | baseline | 31.9 s | 13.3 s | 2.39x |
| 2026-10-04 | 3.14 | PIntSet patch (runtime) | 10.7 s | 9.3 s | 1.15x |
| 2026-10-04 | 3.14t | PIntSet patch (runtime) | 8.1 s | 3.2 s | 2.49x |
| 2026-10-05 | 3.14 | Phase 0 (committed), median of 3, machine loaded (load avg 6–9) | 14.7 s | 11.6 s | 1.24x |
| 2026-10-05 | 3.14t | Phase 0 (committed), median of 3, machine loaded (load avg 7–9) | 10.1 s | 5.2 s | 1.93x |

## Log

- 2026-10-04 — Compared with PDS/HyperFlow; chose Python + free-threaded 3.14t; found `PIntSet` indexing slowdown; wrote design note; created branch.
- 2026-10-05 — Phase 0 fix in place; full suite shows no new failures. Overnight runs hung for hours with no
  output (output piped through `tail`); added per-test timeout + scheduler report, found the datetime slow path.
  Made `bigfile` size configurable (100k in tests): saves ~50 s; the big costs were the two taxi tests and failing downloads.
