# Parallel ProgressiVis: bringing PDS-style parallelism to progressive analytics

*Design note — October 2026. Status: Phase 0 done (branch `parallel-scheduler`);
test-suite cleanup in progress; Phase 1 not started. Day-to-day tracking: [`PLAN.md`](../PLAN.md).*

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

**Take now: task parallelism.** Independent branches run concurrently. Safety
follows PDS's rule — *no two directly connected modules run at the same time* —
but enforced by construction (wave selection) instead of locks.

**Take later: data parallelism.** Split a step's rows across threads and merge
partial results. Fits ProgressiVis well because most of its accumulators are
already mergeable (min, max, sum/mean/variance via Chan's formula, histograms,
KLL sketches).

**Take last: pipeline parallelism.** Producer works on new rows while consumer
reads earlier ones. Requires producers not to reallocate data under readers,
i.e. a change in table storage (see Phase 3).

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
already exists) avoids the copy entirely. The slow path also held the GIL,
which is why threads gave nothing on the standard build.

**Finding 2 — parallelism requires free-threaded Python.** With the GIL, the
best case is ~1.15x. On 3.14t, 4 independent pipelines reach ~2.5x on 4
performance cores.

**Finding 3 — the GIL comes back silently.** On 3.14t, importing `progressivis`
re-enables the GIL because `_datasketches` (used only by `KLLSketch`) does not
declare free-threading support. Runs above used `PYTHON_GIL=0`, safe there
because KLL was not used. Also, `datasketches` and `duckdb` have no 3.14t
wheels and build from source (~10 min).

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

### 6.1 The progressive payoff: more data per quantum

In a progressive system, parallelism does not just finish sooner; it changes
*what the user sees at a fixed latency*. If a quantum processes P times more
rows, then for estimators whose error shrinks like 1/√n (means, variances,
histogram bin frequencies, quantile sketches):

- at the same latency, the error is **√P smaller** (16 cores → 4x tighter), or
- the same accuracy is reached **P times sooner** (16 cores → 16x faster to a
  given confidence interval).

For "until convergence" workloads (k-means, PPCA, t-SNE) more rows per quantum
means fewer quanta to a stable picture. This is the combined ProgressiVis + PDS
value proposition: **interactive latency of progressive analytics, with the
throughput of parallel dataflow.**

### 6.2 Interaction no longer starves the background

Today, when the user interacts, `Scheduler.for_input()` restricts scheduling to
modules reachable from the input; everything else pauses until the latency
budget is met. With many cores, interactive modules can get dedicated cores
while the background computation keeps going.

### 6.3 Where the speedup comes from — and its limits

| Source | Scales with | Realistic ceiling |
|---|---|---|
| Task parallelism (Phase 1) | Width of the dataflow graph | Typically 2–10 concurrent modules; a linear chain gets 1x |
| Data parallelism (Phase 2) | Rows per step × mergeable modules | Tens of cores per module, until memory bandwidth saturates |
| Pipeline parallelism (Phase 3) | Depth of the graph | Small additional factor (≈ chain length) |
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

## 7. Plan

### Phase 0 — single-threaded fixes and a benchmark — done
- Done: `PIntSet.__array__`; contiguous `PIntSet`s read as slices.
- Done: full test suite shows no new failures.
- Done: `scripts/bench_parallel.py`.
- Open: investigate the ~20% scheduler idle time.

### Phase 0.5 — bounded steps and a trustworthy test suite (in progress)
- Done: per-test timeout (`pytest-timeout`, 600 s) and the scheduler state in
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

### Phase 1 — task parallelism ("wave" scheduler)
Current single-thread assumptions to address:
1. Producers grow output tables in place with `np.resize`
   (`storage/numpy.py:95`, `table/column.py:345`), which can reallocate under a
   concurrent reader.
2. Change tracking (`PTableChanges`, `table/tablechanges.py`) is a
   producer-owned log mutated by consumers' `slot.update()` in
   `Module.prepare_run()`.
3. One global `run_number`, incremented per module call
   (`core/scheduler.py:435`), drives readiness checks (`core/module.py:966`).

Design:
- **Select a wave**: ready modules from the toposorted run list such that no
  two are directly connected (PDS's rule, by construction).
- **Serial prepare**: on the scheduler thread, assign run numbers and call
  `prepare_run()` — all change-log mutation stays single-threaded.
- **Parallel run**: only `run_step()` executes on a thread pool. Producers of a
  running module are idle (stable inputs); its consumers are idle (no reads of
  its output mid-write).
- **Serial finish**: `after_run`, tick procs, tracer, state transitions.
- **Interaction**: `for_input()` restricts wave eligibility, and can reserve
  cores for the interactive subgraph instead of pausing the rest.
- Opt-in `Scheduler(parallel=..., max_workers=...)`; modules can declare
  themselves not thread-safe (class attribute) to always run alone.
- Audit global state: name generation, `StorageManager`, tracers, global
  `np.random` use in `RandomPTable`, datasketches/KLL.
- CI: add a 3.14t job and run the test suite with the parallel scheduler; fail
  if the GIL is re-enabled at import.
- **Exit criterion**: correctness on the full suite in parallel mode; speedup
  measured on a ≥ 32-core machine for several realistic notebooks (taxi
  heatmap, scaler demo, PPCA).

### Phase 2 — data parallelism inside modules
- A step's rows are split into k slices processed concurrently, partial results
  merged. Start with Min/Max, Var (Chan's parallel update), Histogram1D/2D,
  KLL (sketch merge), Stats.
- Teach the time predictor about k: step size scales with workers so each
  quantum still fits its time budget.
- Expected: the main route to tens of cores, bounded by memory bandwidth.

### Phase 3 — pipeline parallelism
- Replace `np.resize` growth with chunked, append-only column storage so a
  stable prefix can be read while the producer appends.
- Allow a consumer to run concurrently with its producer on already-committed
  rows (relaxing the Phase 1 adjacency rule).

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
Finding 5 shows two modules where it was not (minutes per step). A sweep of the
scheduler costs the *sum* of the quanta of the modules that run, so latency
grows with the number of modules (the paper acknowledges this).

**Intermediate results have prefix semantics, at best.** For exact incremental
operators (min, max, sum, mean, variance, moments, histograms), the output
after a step should equal the batch result on the rows consumed so far (a
*prefix* of the input in arrival order), and the final output should equal the
batch result. Neither property was stated or tested; the new tests found
`Var` violating it (Finding 8).

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
- **Determinism**: concurrent waves change step interleaving; tests that depend
  on exact step sequences may need tolerance or a deterministic mode. Finding 7
  shows how an edge interleaving (a row arriving in the last step) can hide
  a bug in single-threaded runs; parallel runs will make such cases more frequent.
- **Unbounded steps**: one module with a row-by-row loop stalls the whole
  dataflow (Finding 5); parallelism hides this only partially. The timeout
  report should stay in the test suite permanently.
- **User-written modules** (`doc/custom_modules.md`) may not be thread-safe;
  the opt-out flag and documentation must make this explicit.
- **Bandwidth-bound workloads** may show modest gains on many-core machines;
  set expectations with measured numbers, not core counts.

## References

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
