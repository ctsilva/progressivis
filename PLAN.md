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
- [x] Scheduler idle time: at the end of a sweep with all modules blocked, the scheduler slept 0.2 s even
      when nothing outside the dataflow could unblock it (no input, no data source). Now skipped in that case
      (`core/scheduler.py`, `_end_of_modules`). A linalg test: 0.28 s → 0.04 s

## Test suite: never hang, run fast

- [x] Per-test timeout: `pytest-timeout` in `test` extras, `timeout = 600` in `pyproject.toml`
- [x] `tests/conftest.py`: on timeout, report lists each module's state and its input slots
- [x] `bigfile` size configurable: `progressivis.datasets.bigfile_rows()`,
      env `PROGRESSIVIS_BIGFILE_ROWS`; tests default to 100k rows (`tests/__init__.py`);
      non-default sizes get their own file name (`bigfile_100000.csv`)
- [x] Vectorize datetime storage in `PTable.append` (was a per-row loop; identical results, ~10x faster)
- [x] Vectorize `GroupBy.process_created` (was one `Row` read per row); keeps first-appearance
      group order, which `Aggregate` relies on. `test_03_join::test_outer_pu`: > 300 s → 2.4 s
- [x] Stale test expectations: taxi file now 512k rows (groupby reads count from file metadata,
      expected group count from pandas); `"uniq"` → `"nunique"` in `test_03_aggr`;
      `cast(pd.Index[Any], …)` breaks at runtime with pandas 3 (quoted) in `test_03_join`
- [ ] **Real bug:** deletions propagate nondeterministically in `Stirrer → GroupBy → Aggregate`
      (`test_03_aggr::test_aggregate_1_col_delete`, fails on master too). Sometimes the deleted value is
      subtracted, sometimes not, depending on step timing. GroupBy updates its selection correctly; the
      deletion is lost on the way to Aggregate (selected-view change manager?). Repro: run the test 3 times.
- [ ] 11 test classes are skipped on CI (`skipIf(os.getenv("CI"))`) — that is why CI is green. Several no
      longer need to be (no downloads, fast); review and re-enable
- [x] Smaller, offline test data: `digits` dataset (20k jittered sklearn 8x8 digits, no download) replaces
      the dead MNIST URL in PPCA (145 s → 3 s) and `test_as_array3`; taxi Parquet tests use 50k-row slices
      (`tests.taxi_sample`, env `PROGRESSIVIS_TAXI_ROWS`); the threaded taxi CSV test uses a local 50k-row
      bz2 file instead of the 12.7M-row remote one (300 s timeout → 1 s); linalg tables 100k → 20k rows
- [x] HTTP tests: the server was started by importing `RangeHTTPServer.__main__`, which parses pytest's
      argv and listens on 8000 (tests used 9090). Now `tests.LocalHTTPServer` (thread, free port, ready on
      return); no more fixed sleeps (csv_crash slept 10 s per start/stop). 6/6 pass
- [x] `test_00_storageengine`: non-persistent mmap engine used the path `"None/mmap_storage"` (literal None);
      now gets its own temp dir, always cleaned up (`storage/mmap.py`)
- [x] `test_03_recoverable_csv` (8): pandas 3 str dtype in the test; imputer passed pandas Series to
      `Mean.update` (scalar API) and to datasketches KLL (needs typed ndarray) (`stats/utils.py`);
      expectations now follow progressive semantics (imputed from the chunks already read). 14/14, stable
- [x] **Real bug:** `Var` module was wrong (e.g. 0.074 instead of 0.083 on uniform data): column views report
      `len()` as their id range, so `Mean`/`Var`/`Count`/`Cov` in `stats/online.py` over-counted rows. Inputs are
      now converted to arrays. Found by the new prefix-consistency test.
- [ ] Trap to review: `PColumnSelectedView.__len__` returns `last_id + 1`, not the number of selected rows
- [x] `tests/test_04_progressive_guarantees.py`: prefix consistency (Min, Max, Var, 3 random step-size seeds
      each), eventual exactness, interleaving independence (GroupBy→Aggregate with delete: xfail, Finding 7)

### Known failures on master (macOS, Python 3.14, pandas 3.0.6, pyarrow 25)

- ~~`test_00_storageengine::test_storage_engines`~~ — fixed
- ~~`test_03_aggr` (2)~~ — fixed `"uniq"`; `_delete` is the real bug above
- ~~`test_03_groupby` (5)~~ — fixed (stale expectations)
- ~~`test_03_recoverable_csv` (8)~~ — fixed
- ~~`test_03_csv_over_http`~~ — fixed (test server)
- ~~`test_03_ppca` (7), `test_03_csv::test_as_array3`, `test_03_threaded_csv::test_read_csv_taxis`~~ — fixed (local data)
- ~~`test_03_join::test_outer_pu`~~ — fixed (datetime + GroupBy vectorized, pandas 3 cast)

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
