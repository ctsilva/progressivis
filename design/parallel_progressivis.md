# Parallel ProgressiVis: bringing PDS-style parallelism to progressive analytics

*Design note — October 2026. Status: proposal, nothing implemented yet.*

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

**Finding 4 — idle time.** Every serial run showed CPU/wall ≈ 0.8: each
scheduler spends ~20% of its time idle in asyncio waits. Worth investigating
independently.

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

### Phase 0 — single-threaded fixes and a benchmark (small, do first)
- Add `PIntSet.__array__`; on the read path, convert contiguous `PIntSet`s to
  slices (`to_slice_maybe()`) before indexing storage.
- Run the full test suite; confirm results unchanged.
- Add the experiment as `scripts/bench_parallel.py` (serial vs threaded,
  wall and CPU time, GIL status) so later phases are measured the same way.
- Investigate the ~20% scheduler idle time.

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

## 8. Risks and open questions

- **Free-threaded ecosystem maturity**: every native dependency must declare
  free-threading support or the GIL silently returns (datasketches today).
- **Single-thread overhead of 3.14t** relative to the standard build must be
  measured on our workloads; the parallel scheduler must stay optional.
- **Determinism**: concurrent waves change step interleaving; tests that depend
  on exact step sequences may need tolerance or a deterministic mode.
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
- J.-D. Fekete et al. *Progressive Data Analysis* (book).
  https://www.aviz.fr/Progressive/PDABook
