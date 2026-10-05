# PLAN — Parallel ProgressiVis

Working tracker for the `parallel-scheduler` branch. Rationale, measurements and
full design: [`design/parallel_progressivis.md`](design/parallel_progressivis.md).

Legend: `[x]` done · `[~]` in progress · `[ ]` to do

## Where we stopped (2026-10-05)

- Phase 1 and the design-review follow-up are saved in the working tree, **not
  committed or pushed**. Preserve the pre-existing Phase 1 changes.
- Design critique incorporated in `design/parallel_progressivis.md`: PDS data
  lifetime, ownership and callback contract, sweep barriers, failure/cancellation
  requirements, statistical caveats, evaluation matrix, and Phase 2/3 contracts.
- Four scheduler regressions fixed: deferred consumer waiting on unrelated work;
  lifecycle callbacks overlapping workers; tick callbacks overlapping workers;
  graph mutation/reconnection before old workers finish. All four regression
  tests fail with their safeguards disabled. Modules with lifecycle callbacks
  run exclusively; tick callbacks and graph changes require quiescence.
- Benchmark now supports repeated/alternating one-scheduler comparisons, narrow
  and uneven graphs, first-result availability, update gaps, step duration and
  overruns, and JSON export. Four-shape smoke run and JSON checks pass. These
  metrics do **not** measure UI interaction latency; see the design evaluation
  matrix for missing instrumentation.
- Validation results for this follow-up are recorded below. KLL flakiness also
  occurred on standard Python with one scheduler worker, so it cannot be
  attributed exclusively to free-threaded parallel execution.
- Failure/cancellation cleanup now implemented: supervised worker futures,
  shielded cleanup, once-only ending hooks, aggregated errors, consistent stopped
  flags and rejection of aborted-dataflow restart. `stop()` remains resumable;
  companion coroutines are scoped to the run. Nine lifecycle regression tests.
- Next: run the 3.14t CI job on GitHub; audit hidden storage aliases and custom-module
  access; interaction benchmarks and ≥32-core evaluation.
  Then review/commit Phase 1 and decide whether to share Phases 0–0.5 upstream.
- Run quick tests: `.venv/bin/python -m pytest tests`; parallel scheduler:
  `PROGRESSIVIS_WORKERS=4 ...`; free-threaded:
  `PYTHON_GIL=0 PROGRESSIVIS_WORKERS=4 .venv-ft/bin/python -m pytest tests`.
  pytest's four xdist processes are separate from scheduler workers; `-n 0`
  disables xdist. Local HTTP tests need permission to bind localhost sockets.

### Code-review fixes (2026-10-05)

- Interactive input state (`_module_selection`) was read and reset by workers
  (`Module.run` -> `fix_quantum`/`has_input`) while the event loop changed it:
  possible `TypeError` aborting the scheduler, or a lost selection. The parallel
  loop now computes the quantum before submitting the step
  (`Module.run(run_number, quantum=None)`); `fix_quantum` reads one snapshot.
- Input could change a module during its own step (`Variable.from_input`
  updates `result` and `_has_input` after `await for_input()`). `for_input()`
  now waits until that module has no step running on a worker.
- Two regression tests fail with these fixes disabled. Validation: quick suite
  563 passed (standard and free-threaded, 1 worker); quick + slow 574 passed
  (standard and free-threaded, 4 workers); scoped mypy passes.

### Lifecycle validation (2026-10-05)

- Nine new tests in `tests/test_05_scheduler_lifecycle.py` cover cancellation
  during worker execution and teardown (including repeated cancel calls), late
  worker errors, concurrent failures, companion failures/termination, startup
  and final-hook failures, teardown errors and pause/resume.
- Temporarily restoring legacy run supervision reproduces the cancellation,
  startup, multiple-error and lingering-companion failures; current source restored.
- Standard Python, default scheduler: quick suite **561 passed**, 37 skipped,
  35 subtests passed.
- Standard Python, four scheduler workers, quick + slow: **572 passed**,
  37 skipped, 35 subtests passed.
- Free-threaded Python (`PYTHON_GIL=0`), four scheduler workers: quick suite
  **561 passed**, 37 skipped, 35 subtests passed; slow suite **11 passed**.
- Scoped mypy on scheduler and both new test files passes. Benchmark smoke
  passes all eight graph/worker combinations and produces valid JSON.
- KLL's earlier intermittent comparison failure did not recur in these runs;
  it remains unresolved. Both slow-test runs still print native datasketches/
  nanobind leak diagnostics at process exit.
- All work remains uncommitted. Next: KLL oracle/dependency investigation,
  thread-budget control and free-threaded CI, then interactive/many-core evaluation.

### Design-review validation (2026-10-05)

- Four regression protections tested by temporarily disabling them and restoring
  the source in `finally`: all four fail on the intended ordering assertions.
- Focused parallel scheduler file: 12 tests and 18 subtests pass (standard Python).
- Benchmark smoke: `python scripts/bench_parallel.py 2 1000 --repeats 1
  --skip-upper-bound --json /tmp/progressivis-bench-review.json`; eight runs across
  four shapes, Python-native JSON scalars and first-result timing order verified.
  This is an execution check, not performance evidence.
- Quick suite, standard Python / 1 scheduler worker: 551 passed, 37 skipped,
  33 subtests passed; known `test_kll3` failed (PMF comparison of two sketches).
- Quick suite, standard Python / 4 workers: 552 passed, 37 skipped, 33 subtests passed.
- Quick suite, free-threaded Python / 4 workers: 552 passed, 37 skipped, 33 subtests passed.
- Quick suite, free-threaded Python / 1 worker: 552 passed, 37 skipped, 33 subtests passed.
- `test_kll3` passes when rerun alone on standard Python. No KLL code or tolerances changed.
- Slow suite / 4 workers: 11 passed on standard Python and 11 passed on free-threaded
  Python. Both runs print datasketches/nanobind leak diagnostics at process exit;
  passing tests do not resolve native dependency safety.
- Free-threaded benchmark: K=6, 2,000 rows, two repetitions of fan-out and narrow
  graphs. JSON verified: eight runs, alternating worker order, GIL disabled,
  timing order and overrun-count invariants. Again, smoke data only.
- Scoped mypy (scheduler, benchmark, parallel tests), compilation and diff checks pass.
- Full-suite runs used four xdist processes; counts above retain skipped tests
  explicitly. The initial sandbox run could not collect local HTTP tests;
  subsequent suite runs had localhost access.

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

- [x] Per-test timeout: `pytest-timeout` in `test` extras, 10 s by default, 120 s for slow tests
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
- [x] Finding 7: `test_03_aggr::test_aggregate_1_col_delete` failed depending on timing. Not change
      propagation: `Stirrer` deferred a deletion when the row arrived in the current step and never ran again
      once its input was exhausted. Now keeps pending deletions (`table/stirrer.py`). Interleaving test deletes
      an early row and the last row under random step sizes; fails on the old code
- [ ] `Stirrer`'s `fixed_step_size` parameter is disabled (`if ... and False`, since 2018); 10 tests pass it
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
      each), eventual exactness, interleaving independence (Stirrer→GroupBy→Aggregate with deletions)

- [x] Quick by default: `pytest tests` runs 538 tests in ~12 s (was 18 min).
      - 10 tests over 1 s are marked `slow` and skipped (`pytest -m slow` runs them, `-m ''` runs all)
      - per-test timeout 10 s (120 s for `slow`), so a hang fails in seconds
      - `gc.freeze()` after collection: `ProgressiveTest.tearDown`'s `gc.collect()` went from ~25-50 ms to ~1 ms
      - 4 parallel workers by default (`pytest-xdist`, `-n 0` to debug); dataset files are written to a
        per-process temp name and renamed, so workers can generate them concurrently on a fresh checkout
      - taxi zone lookup CSV cached as dataset `taxi-zone-lookup` (join tests no longer hit the network)
      - note: CI's `coverage run -m pytest` only measures the main process with xdist

### Known failures on master (macOS, Python 3.14, pandas 3.0.6, pyarrow 25)

- ~~`test_00_storageengine::test_storage_engines`~~ — fixed
- ~~`test_03_aggr` (2)~~ — fixed (`"uniq"`; `_delete`: Finding 7)
- ~~`test_03_groupby` (5)~~ — fixed (stale expectations)
- ~~`test_03_recoverable_csv` (8)~~ — fixed
- ~~`test_03_csv_over_http`~~ — fixed (test server)
- ~~`test_03_ppca` (7), `test_03_csv::test_as_array3`, `test_03_threaded_csv::test_read_csv_taxis`~~ — fixed (local data)
- ~~`test_03_join::test_outer_pu`~~ — fixed (datetime + GroupBy vectorized, pandas 3 cast)

## Phase 1 — task parallelism (parallel scheduler)

- [x] Thread-safety audit: module state is per module (slots, change buffers, tracer, predictor; table
      names derive from module names). Shared: numpy's global RNG (RandomPTable, Sample, Stirrer, RangeQuery,
      KernelDensity, BlobsPTable) — thread-safe but order-dependent; mmap temp dir (now locked)
- [x] `Scheduler(workers=N)` / env `PROGRESSIVIS_WORKERS`; default 1 = unchanged serial loop
- [x] Dynamic scheduling within sweeps, with a barrier between sweeps: a module starts on a worker as soon as one is free and no directly
      connected module is running; otherwise it is deferred within the sweep (and so are modules whose
      producer is deferred). `prepare_run`, `start_run`, `after_run`, tick procs stay in the event loop.
      (A first version ran synchronous waves: each wave waited for its slowest module, ~1.1x.)
- [x] Bugs found by running the suite in parallel:
      - `PTableChanges`: consumers of the same table can reset their slot inside their step; now locked,
        and a bookmark is reused wherever it is (times are no longer strictly increasing in a sweep)
      - `MVBlobsPTable` seeded numpy's global RNG: two sources interleaved; now a private RandomState
        (identical data in serial)
      - `PACSVLoader` recovery rewound a shared BytesIO while the previous pyarrow reader could still read
        ahead from it: failed intermittently on free-threaded Python even serially; now an independent BytesIO
- [x] Earlier test suite passed with `PROGRESSIVIS_WORKERS=4` on 3.14 and 3.14t (548 quick + 11 slow);
      current follow-up validation is recorded above
- [x] `tests/test_05_parallel_scheduler.py`: connected modules never overlap (fails when the scheduler is
      sabotaged), modules do run concurrently, exact results, prefix consistency under parallelism
- [x] Intermittent `test_03_kll::test_kll3` failure: a test tolerance problem, not threading or
      datasketches. KLL is randomized; a K=300 PMF is within +-0.011 per sketch (99%), and the test
      compared two independent sketches with atol 0.01. Two batch sketches of the same data, no
      scheduler, exceed it ~3% of the time on both builds. Tests now compare ranks (quantiles, and
      PMFs as cumulative sums) within twice datasketches' rank error: 0/80 failures across
      std/ft x 1/4 workers, and a planted bug (values < 0.05 dropped) still fails 5 of 6 tests
      (`test_kll3` derives its bins from each sketch's own range, so it cannot see that bug).
- [x] Reconsider deferred work before waiting at sweep drain; regression test
- [x] Quiescent boundaries for start/after callbacks, tick callbacks, graph edits; regression tests
- [x] Explicit ownership/lifecycle assumptions documented; no general alias enforcement yet
- [x] Failure/cancellation protocol: stop admission, asynchronously join workers, teardown once,
      restore state and preserve multiple errors; repeated cancellation cannot interrupt cleanup
- [x] Preserve stop/resume; scope companion coroutines to each run; reject restart after abort
- [x] Interactive input with workers: quantum computed in the event loop; `for_input()` waits for
      the module's running step; regression tests
- [ ] Audit aliasing views, custom preparation hooks and unsynchronized external readers
- [ ] Interaction: `for_input()` limits eligibility already (via `_consider_module`); reserve cores later
- [x] Control BLAS/OpenMP threads (oversubscription): with workers > 1, threadpoolctl limits them to
      cores // workers while the scheduler runs (`blas_threads=`, env `PROGRESSIVIS_BLAS_THREADS`,
      0 = unchanged); process-wide, restored at the end of the run; test fails with the limit disabled
- [~] CI job on 3.14t (`free-threaded` in `.github/workflows/python.yml`, plus `workflow_dispatch`):
      `scripts/check_free_threading.py` fails if a module other than `_datasketches` re-enables the
      GIL; then the full suite with `PYTHON_GIL=0`, 4 workers. Steps pass locally (macOS); not yet run
      on GitHub (Linux 3.14t wheels for all dependencies unverified)
- [x] Benchmark result availability, update gaps and step overruns; repeated comparisons and JSON
- [ ] Instrument UI interaction latency, readiness delay, quality targets, memory and change-log backlog
- [ ] Measure on a ≥ 32-core machine (taxi heatmap, scaler demo, PPCA), including latency criteria

Historical measurement before callback-boundary fixes (one scheduler, 4 workers, 1M rows, busy laptop; `scripts/bench_parallel.py 4 1000000`):

| Dataflow | 3.14 (GIL) | 3.14t (no GIL) |
|---|---|---|
| 4 independent pipelines | 1.11x | 2.13x (CPU/wall 3.1) |
| 1 source → 4 Histogram2D + Min/Max/Var | 1.35x | 1.39x |

Upper bound for 4 pipelines (4 separate schedulers in 4 threads, 3.14t): ~2.3x.
The fan-out is limited by the source, which runs alone (all its consumers are connected to it): Phase 3.

## Phase 2 — data parallelism inside modules

- [ ] Module capabilities: append/update/delete, snapshot, partition, merge and numerical tolerance
- [ ] Start append-only Min/Max, Var and fixed-bin histograms; then KLL/Stats with explicit contracts
- [ ] Predictor includes partition/copy/merge and measured scaling; shared task/BLAS resource budget

## Phase 3 — pipeline parallelism

- [ ] Chunked append-only column storage (no `np.resize` reallocation under readers)
- [ ] Define committed versions, reader lifetimes and reclamation, including updates/deletions/schema changes
- [ ] Bound buffers and change logs; define slow-consumer backpressure and multi-input consistency
- [ ] Experiment with bounded input copy and explicit early release before general storage replacement
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

- 2026-10-05 (lifecycle) — Implemented supervised failure/cancellation cleanup,
  once-only module ending, multiple-error retention, companion-task cleanup and
  restart rejection after abort. Preserved resumable stop. Nine real-worker tests;
  restoring legacy run supervision reproduces the cancellation, startup,
  multi-error and lingering-companion regressions. Full validation results are
  recorded in the lifecycle matrix above.

- 2026-10-05 (design review) — Read the supplied CiSE 2011 PDS paper; documented
  ownership, lifecycle boundaries, remaining failure/cancellation work and
  statistical limits. Fixed four scheduler ordering regressions with sabotage
  checks. Added repeated result-availability/overrun benchmarks and JSON export;
  validated standard/free-threaded execution. See the current validation matrix.

- 2026-10-04 — Compared with PDS/HyperFlow; chose Python + free-threaded 3.14t; found `PIntSet` indexing slowdown; wrote design note; created branch.
- 2026-10-05 (later) — Test suite: offline data, fixes (Var, imputer, mmap, HTTP server, idle sleep), quick by
  default (~12 s, 4 xdist workers, 10 s timeout). Finding 7 was Stirrer, not change propagation. Read the
  ProgressiVis paper: no formal guarantees; added §8 Guarantees to the design note and tests for them.
  Phase 1 parallel scheduler: waves (~1.1x) replaced by dynamic scheduling within sweeps (~2.1x on 3.14t).
- 2026-10-05 — Phase 0 fix in place; full suite shows no new failures. Overnight runs hung for hours with no
  output (output piped through `tail`); added per-test timeout + scheduler report, found the datetime slow path.
  Made `bigfile` size configurable (100k in tests): saves ~50 s; the big costs were the two taxi tests and failing downloads.
