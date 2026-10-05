# Parallel ProgressiVis: bringing PDS-style parallelism to progressive analytics

*Design note — October 2026. Status: Phases 0 and 0.5 done; Phase 1 (parallel
scheduler) implemented and being validated (branch `parallel-scheduler`). Day-to-day tracking: [`PLAN.md`](../PLAN.md).*

## 1. Summary

ProgressiVis runs every module of a dataflow in short, time-bounded steps (a
*quantum*, ~0.5 s), so the analyst always has a usable, improving result and can
steer the computation. Today all of this happens on **one core**: the scheduler
is a single asyncio loop that runs one module at a time.

The Parallel Dataflow Streaming framework (PDS, Vo et al. 2010/2011) and its
successor HyperFlow (Vo et al. 2012) showed how a visualization dataflow can
exploit task, pipeline and data parallelism on multicore (and GPU) hardware.
Their goal was throughput; ProgressiVis's goal is bounded latency. The two are
complementary: **parallelism lets each quantum process more data, so the
progressive result converges faster at the same interactive latency.**

This note proposes to add PDS-style parallelism to ProgressiVis, in Python,
targeting the free-threaded CPython build (3.14t), in incremental phases. It
also records a single-threaded performance bug found along the way that gives a
3–4x speedup on its own.

## 2. Three systems compared

| | PDS / VTK (CiSE 2011) | HyperFlow (EGPGV 2012) | ProgressiVis |
|---|---|---|---|
| Goal | Multicore throughput | Heterogeneous CPU+GPU throughput | Bounded latency, steerable, approximate-then-exact |
| Unit streamed | Data blocks / pieces | "Flows" (tokens: data ref + block id) | Change deltas (created/updated/deleted row-index sets) on shared, growing tables |
| Module | Mostly stateless per block, merge at end | Task-Oriented Module with several implementations | Stateful accumulator; must also handle updates and deletions |
| Chunk size decided by | Data decomposition | Data decomposition | **Time**: a throughput predictor sizes each step to fit the quantum |
| Execution | Unified push/pull; centralized scheduler, distributed executives | Event-driven push; flow cache joins inputs | Centralized round-robin over a topological order; time-sliced |
| Ordering | Priority = (block #, topo order) → depth-first | Monotonic flow id → depth-first | Topo order per sweep; one sweep ≈ one "block" |
| Parallelism | Task + pipeline + data (threads) | Task + data + streaming-data (CPU & GPU) | **None at dataflow level** (only inside numpy calls) |
| Concurrency safety | Locks on output connections | Copy-on-write reference-counted data | Not needed — single-threaded |
| Runtime adaptation | Thread count per branch from timings | Device choice from timings | Step size from timings |
| Graph | Static | Cycles allowed | DAG, but editable while running |
| Interaction | Hybrid pull (coarse LOD) + push (refinements) | — | Input-reachable modules get a latency budget |

The closest point of contact is PDS's LOD example: *pull* a coarse result
synchronously, then *push* refinements. ProgressiVis makes that pattern the
default for every module rather than an application-level construction.

What ProgressiVis has that PDS/HyperFlow do not: incremental semantics with
updates and deletions, time (not data size) as the scheduling resource, and a
dataflow that can be modified while running.

## 3. What we take from PDS — and what we don't (yet)

**Take now: task parallelism with explicit data ownership.** Independent branches
run concurrently. PDS locks output connections until consumers release their
inputs (CiSE 2011, pp. 77–78); excluding adjacent executions is a consequence of
that data-lifetime rule. ProgressiVis enforces adjacency exclusion plus producer
ordering within a sweep and a barrier between sweeps. This is sufficient only
under the ownership and callback assumptions below; adjacency alone is not a
proof when outputs alias upstream storage.

PDS also provides `ReleaseInputs()` after copying inputs and `PushNoLock()` when
outputs occupy independent memory. A restricted copy-and-release experiment may
therefore be useful before a general storage redesign. These are capabilities
with explicit data-lifetime obligations, not blanket exemptions from safety.

**Take later: data parallelism.** Split a step's rows across threads and merge
partial results. Fits ProgressiVis well because most of its accumulators are
already mergeable (min, max, sum/mean/variance via Chan's formula, histograms,
KLL sketches).

**Take last: pipeline parallelism.** Producer works on new rows while consumer
reads earlier ones. Requires stable input versions, reader lifetimes and bounded
buffering as well as storage that cannot reallocate under readers (see Phase 3).

**Not adopted:** PDS's push/pull execution model and block-number ordering.
ProgressiVis keeps its own time-quantum scheduler; parallelism only lets more
than one module spend its quantum at once.

**Deferred:** HyperFlow's multiple implementations per module (CPU/GPU).

## 4. Why Python (and not Julia)

Julia offers native threads, compiled loops and multiple dispatch (a natural fit
for HyperFlow-style multiple implementations). We still recommend staying in
Python:

- **JIT warm-up conflicts with the core promise.** The first call of each method
  compiles; in exploratory use (modules added live) that lands inside the first
  quanta, exactly where latency matters most.
- **Users and ecosystem are in Python**: Jupyter, pandas, scikit-learn,
  pyarrow, `ipyprogressivis`, the notebooks and the PDA book.
- **The hard problem is architectural, not linguistic.** Shared growing tables
  and the change-tracking log need the same redesign in any language.
- **Measurements below show the real bottleneck was a Python-level bug**, and
  the GIL ceiling is lifted by free-threaded CPython 3.14t.

If profiling later shows framework overhead dominating, the escape hatch is a
native core (Rust via PyO3, or C++) that releases the GIL — the Polars/pyarrow
pattern — not a full rewrite. Julia remains attractive for a *research
prototype* of the execution model.

## 5. Measurements (October 2026)

**Setup.** Apple Silicon, 4 performance + 6 efficiency cores; CPython 3.14.7,
standard and free-threaded builds, via `uv`. Workload: K = 4 fully independent
pipelines, each `RandomPTable(10 cols, 1M rows) → {Min, Max} → Histogram2D`
plus `Var`, each with its own `Scheduler`, run serially and then one per
thread. Independent schedulers share nothing, so this is an *upper bound* on
task-parallel speedup.

| Build | Serial | Threaded | Speedup |
|---|---|---|---|
| 3.14 (GIL), current code | 33.6 s | 36.6 s | 0.92x |
| 3.14t (no GIL), current code | 31.9 s | 13.3 s | 2.39x |
| 3.14 (GIL), PIntSet fix | 10.7 s | 9.3 s | 1.15x |
| 3.14t (no GIL), PIntSet fix | 8.1 s | 3.2 s | 2.49x |
| 3.14 (GIL), fix committed, median of 3, busy machine | 14.7 s | 11.6 s | 1.24x |
| 3.14t (no GIL), fix committed, median of 3, busy machine | 10.1 s | 5.2 s | 1.93x |

The first four rows are single runs on a quiet machine with the fix applied
as a runtime patch; the last two are medians of three runs with the committed
fix while other applications kept the machine busy (load average 6–9 on 10
cores). Absolute times and speedups vary noticeably with machine load.

**Finding 1 — single-threaded bug.** Profiling showed framework bookkeeping is
negligible; ~70% of time was in two numpy read paths. `table.loc[slice]`
converts the slice to a `PIntSet` (`table/table_base.py:964`), which reaches
`NumpyDataset.__getitem__` (`storage/numpy.py:110`). `PIntSet` defines no
`__array__`, so numpy iterates it as a Python sequence, one int at a time:

| Index type (300k rows) | Time per read |
|---|---|
| `PIntSet` | 12.4 ms |
| `np.ndarray` of ints | 0.14 ms |
| `slice` | ~0 (view) |

Adding `PIntSet.__array__` (via `BitMap.to_array()`) made pipelines **3–4x
faster**; converting contiguous ranges back to slices (`to_slice_maybe()`
already exists) avoids expensive index conversion. The implemented read path
still returns a copy; zero-copy reads require a separate mutation audit.
The slow path also held the GIL,
which is why threads gave nothing on the standard build.

**Finding 2 — parallelism requires free-threaded Python.** With the GIL, the
best case is ~1.15x. On 3.14t, 4 independent pipelines reach ~2.5x on 4
performance cores.

**Finding 3 — the GIL comes back silently.** On 3.14t, importing `progressivis`
re-enables the GIL because `_datasketches` (used only by `KLLSketch`) does not
declare free-threading support. Runs above used `PYTHON_GIL=0`, safe there
because KLL was not used. Also, `datasketches` and `duckdb` have no 3.14t
wheels and build from source (~10 min).

**Phase 1 measurements (parallel scheduler).** One scheduler, 4 workers,
1M rows per source, on the same busy laptop:

| Dataflow | 3.14 (GIL) | 3.14t (no GIL) |
|---|---|---|
| 4 independent pipelines | 1.11x | 2.13x (CPU/wall 3.1) |
| 1 source → 4 `Histogram2D` + `Min`/`Max`/`Var` | 1.35x | 1.39x |

The upper bound for the first case (4 separate schedulers in 4 threads) is
~2.3x. The fan-out case is limited by its source, which runs alone because
every other module is connected to it; pipeline parallelism (Phase 3) is what
lifts that limit.

**Finding 4 — idle time (fixed).** Every serial run showed CPU/wall ≈ 0.8.
Cause: when all modules were blocked at the end of a sweep, the scheduler
slept 0.2 s, even when nothing outside the dataflow (no input module, no data
source) could unblock it, i.e. just before terminating. It now skips the sleep
in that case. Small dataflows were dominated by it: a `linalg` test went from
0.28 s to 0.04 s, and 230 scheduler/linalg tests from ~90 s to 17 s.

**Finding 5 — row-by-row Python loops break the time quantum ("hangs").**
The full test suite hung for hours. With a per-test timeout and a report of
the scheduler state at timeout, the cause turned out not to be a deadlock but
single `run_step` calls lasting minutes:

- `PTable.append` stored datetime columns with a per-row Python loop when the
  vectorized copy failed (`table/table.py`), which it does with pandas 3 /
  pyarrow 25.
- `GroupBy.process_created` read each row's key through `Row` objects, one
  `PIntSet` per row (`table/group_by.py`).

Both are now vectorized. `test_03_join::test_outer_pu` went from > 300 s
(timeout) to 2.4 s, results unchanged (and group creation order preserved,
which `Aggregate` relies on). The lesson matters for the design: **a module
whose step cost is not proportional to its step size defeats the time
predictor**, so no quantum (and no parallel scheduler) can keep it bounded.
Every `run_step` must be vectorized over its chunk.

**Finding 6 — CI is green partly because it skips the hard tests.** 11 test
classes are skipped when `CI` is set: aggregate, group-by, join, recoverable
CSV, PPCA, both HTTP-server suites, threaded CSV, KLL, MNIST and a long
correlation test. Locally (macOS, Python 3.14, pandas 3.0.6, pyarrow 25) the
suite had 29 failures, all also present on `master`: stale expectations (a
300k-row taxi file that is now downloaded with 512k rows; an aggregator
renamed `uniq` → `nunique`; a pandas 3 runtime-typing incompatibility), a dead
download URL (MNIST on datahub.io), a flaky local HTTP server, and two real
bugs (Findings 5 and 7). Phase 1 needs a test suite that runs these
paths, so cleaning it up comes first.

**Finding 7 — a requested deletion could silently never happen (fixed).** In
`Stirrer → GroupBy → Aggregate`, a deleted row was sometimes subtracted from the
aggregate and sometimes not, depending on how the run was split into steps.
The first diagnosis (deletions lost between `GroupBy` and `Aggregate`) was
wrong: change propagation was correct. `Stirrer`, the test module that
injects deletions, deferred a deletion when the row arrived in the current
step, then never ran again once its input was exhausted; whether the row
arrived in the last step depended on timing (its `fixed_step_size` parameter
has been disabled since 2018). It now keeps pending deletions and stays ready
until they are applied. The interleaving test deletes both an early row and
the last row under random step sizes, and fails on the old code. The lesson
for Phase 1 stands: results must not depend on the interleaving, and tests
must force the edge interleavings (last step, empty steps) on purpose.

**Finding 8 — the `Var` module returned wrong variances.** On uniform data it
reported 0.074 instead of 0.083 after 20k rows. Column views report `len()`
as the size of their id range, not the number of selected rows, so the online
statistics (`Mean`, `Var`, `Count`, `Cov` in `stats/online.py`) over-counted.
No existing test compared `Var` with numpy; the new prefix-consistency test
(§8) caught it at the second step. Inputs are now converted to arrays; the
`len()` semantics of column views remains a trap to review.

**Other bugs fixed while cleaning the tests.** The CSV imputer passed whole
pandas Series where scalars or typed arrays were expected (mean, median
strategies); a non-persistent mmap storage engine created its files under a
directory literally named `None`; the HTTP test server never started under
pytest (it parsed pytest's command line and listened on another port).

**Test-suite size.** The generated `bigfile` (1M rows × 30 columns, 556 MB plus
compressed and Parquet copies) is now configurable (`PROGRESSIVIS_BIGFILE_ROWS`,
100k in tests). That saved ~50 s; the dominant costs of the 18-minute local run
were the two tests stuck in the per-row loops (600 s) and failing downloads.

## 6. Why this matters: tens to hundreds of cores

Desktops now ship with 16–24 cores, workstations with 64–96, and servers with
128–192. A single-threaded ProgressiVis uses one of them. The opportunity is
large, but it is worth being precise about where the speedup comes from.

### 6.1 The progressive payoff is a hypothesis to measure

Task parallelism increases aggregate throughput across branches; it need not
increase the rows processed by a particular module per quantum. Contention can
even increase step duration. The intended payoff is more useful results at a
fixed latency, which must be measured at the output the analyst actually uses.

If the measured number of relevant observations at that output increases by P,
then an estimator with 1/√n sampling error can have √P smaller uncertainty,
under its sampling and moment assumptions. P is a throughput ratio, not a core
count. File-order prefixes generally do not support population-confidence claims.

Sampling uncertainty and algorithmic approximation error are distinct. KLL's
normalized rank error is governed by its configured K, not a general 1/√n law
as the stream grows (see the DataSketches reference). More rows also do not by
themselves prove faster convergence for k-means, PPCA or t-SNE. Measure time to
a declared quality target for each algorithm, including preprocessing and merges.

### 6.2 Future interaction resource allocation

Today, when the user interacts, `Scheduler.for_input()` restricts scheduling to
modules reachable from the input; everything else pauses until the latency
budget is met. With many cores, interactive modules can get dedicated cores
while the background computation keeps going. This is future work: Phase 1
retains the existing eligibility restriction, and does not preempt running steps.

### 6.3 Where the speedup comes from — and its limits

| Source | Scales with | Realistic ceiling |
|---|---|---|
| Task parallelism (Phase 1) | Width of the dataflow graph | Typically 2–10 concurrent modules; a linear chain gets 1x |
| Data parallelism (Phase 2) | Rows per step × mergeable modules | Tens of cores per module, until memory bandwidth saturates |
| Pipeline parallelism (Phase 3) | Stage balance and graph depth | Ideal throughput ratio ≤ sum of stage costs / slowest stage cost |
| Multiple concurrent dataflows / users | Number of sessions | Linear, mostly independent |

Key consequences:

- **Task parallelism alone cannot use 100 cores.** Real ProgressiVis graphs have
  5–30 modules and limited width. It is the right *first* step because it
  forces the thread-safety work, but data parallelism is what reaches tens of
  cores.
- **Memory bandwidth is the eventual ceiling.** Min/max/histogram/variance are
  streaming scans. A 12-channel DDR5-4800 server provides ~460 GB/s total, i.e.
  ~3.6 GB/s per core on a 128-core part, while one core can stream a simple
  reduction at roughly 10 GB/s. Bandwidth-bound modules will therefore saturate
  at a few dozen cores; compute-heavy modules (k-means, PPCA, t-SNE, KDE,
  2D histograms with many bins) scale further.
- **Avoid oversubscription.** numpy/BLAS (via scikit-learn, PPCA) already spawn
  threads; the scheduler must control them (e.g. `threadpoolctl`) so total
  threads ≈ cores.
- **The measured 2.5x on 4 cores is a best case** for fully independent
  branches; shared producers will reduce it. Numbers on a real many-core
  machine are the first thing to obtain (Phase 1 exit criterion).

### 6.4 Evaluation and acceptance criteria

Compare workers=1 and N on the same build and graph. Include independent
pipelines, shared-source fan-out, a narrow source-to-reduction chain, unequal
branch costs, and a realistic notebook receiving interactions during background
computation. The first four shapes are implemented in `scripts/bench_parallel.py`;
the interaction workload remains to be built. Distinguish worker threads from
pytest workers. Record GIL status, numerical-library thread settings, machine
load, dataset, and warm-up policy. Use at least three repetitions, alternating
serial/parallel order; retain individual runs, not just their median.

| Metric | Meaning | Current benchmark coverage |
|---|---|---|
| Completion wall time and CPU/wall | Throughput and actual CPU use | Implemented |
| First productive result, per terminal analytic module | Earliest worker output; also report time until all have produced | Implemented; not UI presentation |
| p95 gaps between productive results | Availability cadence | Implemented; unavailable with fewer than two results |
| Step p95/max and overruns | Actual duration versus requested quantum | Implemented around `run_step` |
| First visible result and p50/p95 input-to-visible-update | End-to-end interaction latency | Requires notebook/UI instrumentation and input generation IDs |
| Ready-to-start delay | Scheduler waiting rather than computation | Pending readiness instrumentation |
| Time to specified error/quality | Progressive utility | Pending algorithm-specific oracle/target |
| Peak memory and change-log backlog | Cost of buffering and slow consumers | Pending storage instrumentation |

Example: `python scripts/bench_parallel.py 4 1000000 --repeats 3 --json results.json`.
The optional independent-scheduler upper-bound comparison remains a single trial;
`--skip-upper-bound` skips it. The JSON contains the repeated one-scheduler runs.
Instrumentation wraps steps rather than lifecycle callbacks, since unrestricted
callbacks require quiescence and would change the scheduling being measured.
An overrun count describes observed behavior; it does not enforce a deadline.

Phase 1 acceptance requires correctness under controlled adversarial interleavings
and a declared latency budget for a representative interactive workload, not
just faster completion. Publish both latency and throughput, with serial
regressions visible. Do not infer scaling on ≥32 cores from the laptop results;
those measurements and the interaction budget are still outstanding.

## 7. Plan

### Phase 0 — single-threaded fixes and a benchmark — done
- Done: `PIntSet.__array__`; contiguous `PIntSet`s read as slices.
- Done: full test suite shows no new failures.
- Done: `scripts/bench_parallel.py`.
- Done: avoid the unnecessary final idle sleep (Finding 4).

### Phase 0.5 — test-suite repair done; broader audits remain
- Done: per-test timeout (`pytest-timeout`, 10 s; 120 s for slow tests) and the scheduler state in
  timeout reports (`tests/conftest.py`), so a hang is a failure with a diagnosis.
- Done: smaller generated datasets for tests (`PROGRESSIVIS_BIGFILE_ROWS`).
- Done: vectorize the datetime path of `PTable.append` and `GroupBy` (Finding 5).
- Done: fix stale test expectations (taxi row counts, `nunique`, pandas 3 typing).
- Done: offline, smaller test data (generated digits instead of MNIST; 50k-row
  taxi slices; local taxi CSV); reliable in-process HTTP test server; fixes
  listed in Findings 4 and 8.
- Done: prefix-consistency and interleaving tests
  (`tests/test_04_progressive_guarantees.py`).
- Done: Finding 7 fixed (`Stirrer` pending deletions); no expected failures left.
- To do: audit other modules for row-by-row loops in `run_step`.
- To do: run the CI-skipped tests somewhere (e.g. a scheduled job with cached
  datasets), so they cannot rot silently.

### Phase 1 — task parallelism (parallel scheduler) — implemented, being validated
Single-thread assumptions that had to be addressed:
1. Producers grow output tables in place with `np.resize`, which can
   reallocate under a concurrent reader. Addressed by never running a module
   concurrently with a module directly connected to it, preserving producer
   order and draining each sweep. Aliased views need the ownership contract below.
2. Change tracking (`PTableChanges`) is a producer-owned log mutated by its
   consumers. Not only in `prepare_run()`, as first assumed: `@process_slot`
   also resets and updates slots *inside* `run_step()`. Two consumers of the
   same table can do so concurrently, so the log is now locked, and a consumer
   can re-register at an earlier time than another concurrent consumer
   (bookmarks are reused wherever they are).
3. One global `run_number`: still assigned serially, in the event loop.

Design as implemented (`Scheduler(workers=N)`, or env `PROGRESSIVIS_WORKERS`;
default 1 keeps the serial loop unchanged):
- The sweep visits modules in topological order. A module **starts** on a
  worker thread as soon as a worker is free and no module directly connected
  to it is running; otherwise it is **deferred** to later in the same sweep,
  as is any module whose producer is deferred, so each module still runs
  after its producers in every sweep.
- `prepare_run()`, `start_run()`, `after_run()` and tick procs run in the
  event loop; only `Module.run()` (the step) runs on the thread pool.
- Scheduling is dynamic **within a sweep**, with a barrier between sweeps.
  Deferred work is reconsidered before waiting for unrelated running tasks at
  the sweep boundary. This is not a general priority-ready-queue scheduler.
- Modules with registered start/after callbacks or subclass overrides of those
  hooks run exclusively. Tick callbacks wait for all active workers. Graph
  changes drain the old sweep before reconnection and teardown. Loop/idle hooks
  execute after the sweep drains. These conservative boundaries may reduce
  throughput; ordinary callback-free modules retain task parallelism.
- A first version ran **synchronous waves** (start a set of independent
  modules, wait for all). Each wave waited for its slowest member, and gave
  ~1.1x; the initial dynamic version gave ~2.1x on the same dataflow (§5).
  These historical figures predate the callback-boundary fixes.
- Interaction: `for_input()` already limits which modules are considered;
  reserving cores for the interactive subgraph is future work.

Earlier validation: the full test suite passed with 4 workers on 3.14 and
3.14t, plus dedicated tests (`tests/test_05_parallel_scheduler.py`): directly
connected modules never run at the same time (the test fails when the
scheduler is sabotaged), unconnected modules do, and results stay exact and
prefix-consistent. Running the suite in parallel found three more bugs: the
change-log ordering above, a module seeding numpy's global random generator,
and a CSV loader rewinding a stream still read by pyarrow threads (failing on
free-threaded Python even without the parallel scheduler).

Additional regression tests cover the deferred-consumer delay, lifecycle and
tick callback overlap, and graph mutation before worker completion. Each fails
with its protection disabled. Current run counts and remaining failures are
tracked in `PLAN.md`; passing numerical tests alone is not a safety proof.

#### Phase 1 ownership and lifecycle contract

- A module owns mutations of its outputs and internal state. Inputs are read-only
  to consumers, except shared change-log operations explicitly synchronized by
  the framework. Directly connected modules do not run simultaneously.
- Outputs can alias upstream storage (`PTableSelectedView`, for example).
  Producer-before-consumer ordering and the sweep barrier protect ordinary
  ancestor chains. Input mutation through a sibling view or hidden shared state
  violates the contract; the scheduler does not discover storage aliases.
  Audit such modules before enabling parallel execution.
- Register lifecycle callbacks before starting execution. They may access the
  graph at the conservative boundaries above. Arbitrary background coroutines,
  notebook readers and module-created threads do not acquire safety merely by
  running on the event loop; use owned snapshots or arrange a quiescent boundary.
- Custom `prepare_run`/readiness hooks must respect module-local/input access.
  Only start/after hook overrides currently trigger exclusive execution.
  A general per-module capability/opt-out API is still pending; use workers=1
  when a module cannot meet the contract.
- A run number is an assigned logical identifier, not a global commit version.
  Completion order may differ; it must not be used to infer cross-module snapshots.

#### Failure, cancellation and pause contract

Implemented and covered by `tests/test_05_scheduler_lifecycle.py`:

- `stop()` requests a resumable pause. It stops admission of unstarted work;
  await the existing `start()` task to know active steps have finished. Pending
  work is reconsidered on the next `start()`. Pausing does not end live modules.
- Worker or companion-coroutine failure, startup failure, or cancellation of
  the `start()` task aborts the dataflow. Stop admitting work, cancel and await
  companion tasks, asynchronously join active workers, then end modules. An
  aborted scheduler rejects restart: create a fresh scheduler/dataflow because
  output may be partially mutated and there is no transactional rollback.
- Cancellation of an asyncio waiter does not stop its OS thread. Worker futures
  remain supervised until completion. Cleanup runs in a shielded task, so
  repeated `cancel()` calls do not interrupt joining or module teardown; the
  event loop remains available while workers finish. A non-returning worker,
  non-cooperative coroutine, or hanging ending hook can still prevent shutdown.
- Module ending hooks run once per module instance, after worker quiescence;
  cancellation during an ending hook awaits that same invocation. Failure in one
  hook does not prevent cleanup of other modules. The registry uses weak module
  keys so successful teardown does not retain removed modules indefinitely.
- Run-state flags and the executor are cleared on exit, including startup errors.
  A single error propagates unchanged; multiple failures are available through
  `progressivis.core.scheduler.SchedulerRunError.errors` (compatible with Python
  3.10). Cancellation remains `CancelledError`, with collected worker/cleanup
  errors chained as its cause. Completion callbacks do not run for a failed batch.
- `start(coros=...)` coroutines belong to this run: unfinished companions are
  cancelled and awaited on normal completion or pause as well as abort. They
  must cooperate with cancellation and must not assume they outlive the dataflow.
  Independent work should be created and supervised by its caller instead.

Tests exercise real worker threads, two concurrent worker failures, a companion
failure, repeated cancellation during work and teardown, startup/final-hook
failures, normal completion, and pause/resume. A bounded worker in the tests waits
for an event-loop timer, detecting a blocking join. These checks do not establish
native-library free-threading safety or bounded shutdown for arbitrary modules.

Remaining: an intermittent KLL sketch test failure seen both with 3.14t /
4 workers and standard Python / 1 worker (`datasketches` is not declared free-threading-safe, and KLL results also
depend on chunking; not yet separated); BLAS
thread control; a CI job on 3.14t; measurements on a ≥ 32-core machine with
realistic notebooks (exit criterion).

### Phase 2 — data parallelism inside modules
- Define capabilities first: allowed input changes (append/update/delete), input
  snapshot, partition rule, partial-state ownership, merge operation, numeric
  tolerance and reproducibility. Mergeability for inserts is not deletion support.
- Start with append-only Min/Max, Var (Chan's parallel update), and fixed-bin
  Histogram1D/2D; all partitions must use the same bin boundaries. KLL and more
  general Stats follow only with dependency safety and approximation contracts.
- Include partitioning, copying, contention and merging in the time predictor.
  Measure scaling with k rather than multiplying step size by thread count.
  Scheduler tasks and BLAS/numerical kernels must share one resource budget;
  avoid independent nested pools and account for multiple concurrent schedulers.
- Expected: the main route to tens of cores, bounded by memory bandwidth.

### Phase 3 — pipeline parallelism
- Replace `np.resize` growth with chunked, append-only column storage so a
  stable prefix can be read while the producer appends.
- Allow a consumer to run concurrently with its producer on already-committed
  rows (relaxing the Phase 1 adjacency rule).
- Define committed versions and reader lifetimes. Updates/deletions, selection
  changes and schema changes require versioning or exclusion; append-only
  allocation does not solve them. Reclaim chunks only after the last reader.
- Bound retained chunks and change logs; apply backpressure when consumers lag.
  Define what a multi-input module may combine and whether it requires matching
  versions. A slow or abandoned consumer must not retain unbounded memory.
- First experiment: copy a bounded chunk into consumer-owned memory, then
  explicitly release its read dependency before computing (PDS `ReleaseInputs`).
  Measure copy cost, memory and source overlap before changing general storage.
  This is a future opt-in capability, not an exception to current adjacency rules.

### Phase 4 — later
- HyperFlow-style multiple implementations per module (e.g. CuPy), chosen at
  runtime from measured timings.
- Native core (Rust/PyO3) for change management only if profiling justifies it.

## 8. Guarantees: what ProgressiVis promises, and what we should check

The ProgressiVis paper (Fekete & Poli, TVCG) describes the mechanisms but
states no formal guarantees. Reading it together with the code and our test
results:

**Latency is engineered, not guaranteed.** Each module receives a quantum
(0.5 s, 0.1 s in interactive mode) and a time predictor converts it into a
number of items, assuming cost linear in the items processed. Nothing preempts
a module that overruns; the quantum holds only if the predictor is right.
Finding 5 shows two modules where it was not (minutes per step). In serial mode,
a sweep costs the sum of actual step durations plus overhead; parallel mode
depends on dependencies, contention and quiescent boundaries. Neither is an
end-to-end latency bound, and no running step is preempted for new interaction.

**Intermediate results have prefix semantics, at best.** For exact incremental
operators (min, max, sum, mean, variance, moments, histograms), the output
after a step should equal the batch result on the rows consumed so far (a
*prefix* of the input in arrival order), and the final output should equal the
batch result within a declared floating-point tolerance. With updates/deletions,
the oracle must instead use the logical state after consumed change events, not
just a row prefix. The new append-only tests found `Var` violating consistency
(Finding 8); broad update/delete coverage remains to be added.

**Early pictures are not samples unless the input is shuffled.** The prefix
is in file order. The NYC taxi files are ordered by time, so the early heatmap
shows the first days of the month, not a random fraction of it. Statistical
statements (Hoeffding bounds on histogram bin frequencies, DKW bounds on
quantiles, 1/√n confidence intervals on means) apply only to random-order
input; the paper's quality indicators are deliberately not confidence
intervals ("quality should increase until it plateaus; the converse is not
always true").

**No consistency across modules.** Modules lag each other (the paper's
"Synchronization" discussion: values normalized with a min/max that has not
caught up can fall outside [0, 1]). A displayed result may combine inputs at
different progress points.

**What we propose to check, and later to guarantee:**

1. *Bounded steps*: every test runs under a timeout, and the scheduler state
   is reported on overrun (done). Next: flag any `run_step` that exceeds a
   multiple of its quantum, so overruns are visible even when tests pass.
2. *Prefix consistency*: for exact operators, after every step, the output
   equals the batch result on the rows consumed so far; checked under random
   step sizes, with creations, updates and deletions.
3. *Eventual exactness*: at termination, the output equals the batch result
   (most existing tests check only this).
4. *Interleaving independence*: the final result does not depend on how the
   run is split into steps; essential before Phase 1 adds more interleavings.
5. *Statistical meaning* (later): optional shuffled ingestion, and confidence
   bounds for modules where they exist.

## 9. Risks and open questions

- **Free-threaded ecosystem maturity**: every native dependency must declare
  free-threading support or the GIL silently returns (datasketches today).
- **Single-thread overhead of 3.14t** relative to the standard build must be
  measured on our workloads; the parallel scheduler must stay optional.
- **Determinism**: concurrent execution changes step interleaving; tests that depend
  on exact step sequences may need tolerance or a deterministic mode. Finding 7
  shows how an edge interleaving (a row arriving in the last step) can hide
  a bug in single-threaded runs; parallel runs will make such cases more frequent.
- **Unbounded steps**: one module with a row-by-row loop stalls the whole
  dataflow (Finding 5); parallelism hides this only partially. The timeout
  report should stay in the test suite permanently.
- **User-written modules** (`doc/custom_modules.md`) may not be thread-safe;
  a per-module opt-out/capability API remains to be designed. Until then the
  whole-scheduler workers=1 setting is the fallback.
- **Bandwidth-bound workloads** may show modest gains on many-core machines;
  set expectations with measured numbers, not core counts.

## References

- Apache DataSketches. *KLL Sketch Accuracy and Size Vs K and N.*
  https://datasketches.apache.org/docs/KLL/KLLAccuracyAndSize.html
- H. T. Vo, J. L. D. Comba, B. Geveci, C. T. Silva. *Streaming-Enabled Parallel
  Data Flow Framework in the Visualization Toolkit.* Computing in Science &
  Engineering, Sept/Oct 2011.
- H. T. Vo, D. K. Osmari, B. Summa, J. L. D. Comba, V. Pascucci, C. T. Silva.
  *Streaming-Enabled Parallel Dataflow Architecture for Multicore Systems.*
  Computer Graphics Forum 29(3), 2010.
- H. T. Vo, D. K. Osmari, J. Comba, P. Lindstrom, C. T. Silva. *HyperFlow: A
  Heterogeneous Dataflow Architecture.* Eurographics Symposium on Parallel
  Graphics and Visualization, 2012.
- J.-D. Fekete, C. Poli. *ProgressiVis: A Language and Environment for Progressive
  Data Analysis and Visualization.* IEEE TVCG (author version, HAL).
- J.-D. Fekete et al. *Progressive Data Analysis* (book).
  https://www.aviz.fr/Progressive/PDABook
