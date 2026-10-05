"""
Upper bound on task-parallel speedup for ProgressiVis.

Runs K fully independent pipelines, each with its own Scheduler:
  RandomPTable -> {Min, Max} -> Histogram2D -> Tick
  RandomPTable -> Var -> Tick
first one after another (serial), then each in its own OS thread
(each thread has its own asyncio loop). Independent schedulers share no
tables or change logs, so this measures what the interpreter (GIL or not)
allows, including all of the framework's per-step overhead.

On a free-threaded build (3.14t) the GIL is re-enabled at import time by any
extension that does not declare free-threading support (datasketches today);
set PYTHON_GIL=0 to override. The script prints the effective GIL status.

It then builds the same work inside ONE scheduler (workers=1 vs workers=K):
"pipelines" (K independent pipelines) and "fanout" (one source feeding K
Histogram2D modules plus Min/Max/Var), measuring the scheduler itself.
Also supports a narrow graph and unequal branch sizes. Reports result availability
and step overruns; these are not measurements of UI interaction latency.

Usage: python scripts/bench_parallel.py [K] [ROWS] [--repeats 3] [--json PATH]
"""

from __future__ import annotations

import sys
import argparse
import json
from pathlib import Path
from statistics import median
import sysconfig
import threading
import time
import warnings
from typing import Any

import numpy as np

warnings.filterwarnings("ignore")

from progressivis import (  # noqa: E402
    Scheduler,
    RandomPTable,
    Min,
    Max,
    Var,
    Histogram2D,
    Tick,
)
from progressivis.core import aio  # noqa: E402


def build(rows: int) -> Scheduler:
    s = Scheduler(workers=1)
    rnd = RandomPTable(10, rows=rows, scheduler=s)
    min_ = Min(scheduler=s)
    min_.input[0] = rnd.output.result
    max_ = Max(scheduler=s)
    max_.input[0] = rnd.output.result
    h2d = Histogram2D("_1", "_2", xbins=256, ybins=256, scheduler=s)
    h2d.input[0] = rnd.output.result
    h2d.input.min = min_.output.result
    h2d.input.max = max_.output.result
    t1 = Tick(scheduler=s)
    t1.input[0] = h2d.output.result
    var = Var(scheduler=s)
    var.input[0] = rnd.output.result
    t2 = Tick(scheduler=s)
    t2.input[0] = var.output.result
    return s


def run_one(rows: int) -> None:
    aio.run(build(rows).start())


def build_into(s: Scheduler, rows: int) -> None:
    "Same pipeline as build(), added to an existing scheduler"
    rnd = RandomPTable(10, rows=rows, scheduler=s)
    min_ = Min(scheduler=s)
    min_.input[0] = rnd.output.result
    max_ = Max(scheduler=s)
    max_.input[0] = rnd.output.result
    h2d = Histogram2D("_1", "_2", xbins=256, ybins=256, scheduler=s)
    h2d.input[0] = rnd.output.result
    h2d.input.min = min_.output.result
    h2d.input.max = max_.output.result
    t1 = Tick(scheduler=s)
    t1.input[0] = h2d.output.result
    var = Var(scheduler=s)
    var.input[0] = rnd.output.result
    t2 = Tick(scheduler=s)
    t2.input[0] = var.output.result


def build_fanout(s: Scheduler, rows: int, k: int) -> None:
    "One source feeding k histograms (on different column pairs) plus Min/Max/Var"
    rnd = RandomPTable(max(10, 2 * k), rows=rows, scheduler=s)
    min_ = Min(scheduler=s)
    min_.input[0] = rnd.output.result
    max_ = Max(scheduler=s)
    max_.input[0] = rnd.output.result
    var = Var(scheduler=s)
    var.input[0] = rnd.output.result
    t = Tick(scheduler=s)
    t.input[0] = var.output.result
    for i in range(k):
        h2d = Histogram2D(f"_{2 * i + 1}", f"_{2 * i + 2}", xbins=256, ybins=256, scheduler=s)
        h2d.input[0] = rnd.output.result
        h2d.input.min = min_.output.result
        h2d.input.max = max_.output.result
        th = Tick(scheduler=s)
        th.input[0] = h2d.output.result


def in_scheduler(shape: str, k: int, rows: int, workers: int) -> dict[str, Any]:
    "Build the dataflow in ONE scheduler with the given number of workers"
    s = Scheduler(workers=workers)
    if shape in ("pipelines", "uneven"):
        for i in range(k):
            build_into(s, rows if shape == "pipelines" or i == 0 else max(1, rows // 4))
    elif shape == "narrow":
        rnd = RandomPTable(10, rows=rows, scheduler=s)
        var = Var(scheduler=s)
        var.input[0] = rnd.output.result
        tick = Tick(scheduler=s)
        tick.input[0] = var.output.result
    elif shape == "fanout":
        build_fanout(s, rows, k)
    else:
        raise ValueError(shape)
    s.commit()
    # Wrap steps, not lifecycle callbacks: callbacks intentionally introduce
    # quiescent boundaries. Each module owns its trace until workers finish.
    traces: dict[str, list[tuple[float, float, float, bool]]] = {}
    outputs = []
    for module in s.modules().values():
        trace: list[tuple[float, float, float, bool]] = []
        traces[module.name] = trace
        if isinstance(module, (Histogram2D, Var)):
            outputs.append(module.name)

        def timed_step(run_number: int, step_size: int, quantum: float,
                       original: Any = module.run_step, samples: Any = trace) -> Any:
            begin = time.perf_counter()
            result = original(run_number, step_size, quantum)
            samples.append((begin, time.perf_counter(), quantum, result.steps_run > 0))
            return result

        module.run_step = timed_step  # type: ignore[method-assign]
    t0, c0 = time.perf_counter(), time.process_time()
    aio.run(s.start())
    wall, cpu = time.perf_counter() - t0, time.process_time() - c0
    samples = [sample for trace in traces.values() for sample in trace]
    durations = [end - begin for begin, end, _, _ in samples]
    first = {
        name: next((end - t0 for _, end, _, productive in traces[name] if productive), None)
        for name in outputs
    }
    available = [value for value in first.values() if value is not None]
    gaps: list[float] = []
    for name in outputs:
        ends = [end for _, end, _, productive in traces[name] if productive]
        gaps.extend(b - a for a, b in zip(ends, ends[1:]))
    return {
        "shape": shape, "workers": workers, "wall_s": wall, "cpu_s": cpu,
        "first_result_s": min(available) if available else None,
        "all_first_results_s": max(available) if len(available) == len(outputs) and available else None,
        "first_results_by_module_s": first,
        "step_count": len(samples),
        "step_p95_s": float(np.percentile(durations, 95)) if durations else None,
        "step_max_s": max(durations, default=None),
        "step_overruns": sum(bool(end - begin > quantum) for begin, end, quantum, _ in samples),
        "result_gap_p95_s": float(np.percentile(gaps, 95)) if gaps else None,
    }


def serial(k: int, rows: int) -> tuple[float, float]:
    t0, c0 = time.perf_counter(), time.process_time()
    for _ in range(k):
        run_one(rows)
    return time.perf_counter() - t0, time.process_time() - c0


def threaded(k: int, rows: int) -> tuple[float, float]:
    errors: list[BaseException] = []

    def target() -> None:
        try:
            run_one(rows)
        except BaseException as e:  # surface failures from worker threads
            errors.append(e)

    threads = [threading.Thread(target=target) for _ in range(k)]
    t0, c0 = time.perf_counter(), time.process_time()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed, cpu = time.perf_counter() - t0, time.process_time() - c0
    if errors:
        raise RuntimeError(f"{len(errors)} thread(s) failed: {errors[0]!r}")
    return elapsed, cpu


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("k", type=int, nargs="?", default=4)
    parser.add_argument("rows", type=int, nargs="?", default=2_000_000)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--skip-upper-bound", action="store_true")
    parser.add_argument("--shapes", nargs="+", choices=("pipelines", "fanout", "narrow", "uneven"),
                        default=["pipelines", "fanout", "narrow", "uneven"])
    parser.add_argument("--json", type=Path)
    args = parser.parse_args()
    k, rows = args.k, args.rows
    if min(k, rows, args.repeats) < 1:
        parser.error("k, rows and repeats must be positive")
    gil = getattr(sys, "_is_gil_enabled", lambda: True)()
    ft_build = bool(sysconfig.get_config_var("Py_GIL_DISABLED"))
    print(f"python {sys.version.split()[0]}  free-threaded build={ft_build}  "
          f"GIL enabled={gil}  K={k}  rows={rows:,}")
    if ft_build and gil:
        print("warning: GIL re-enabled by an extension; rerun with PYTHON_GIL=0")
    run_one(max(1, rows // 10))  # warm-up: imports, first-call costs
    if not args.skip_upper_bound:
        ts, cs = serial(k, rows)
        tp, cp = threaded(k, rows)
        print(f"serial   wall {ts:7.2f}s  cpu {cs:7.2f}s  (cpu/wall {cs / ts:4.2f})")
        print(f"threaded wall {tp:7.2f}s  cpu {cp:7.2f}s  (cpu/wall {cp / tp:4.2f})"
              f"   speedup {ts / tp:4.2f}x  (ideal {k}x; single upper-bound trial)")
    records = []
    print("one scheduler (result availability, not UI latency):")
    for shape in args.shapes:
        for repeat in range(args.repeats):
            # Alternate order to reduce systematic warm-up/load bias.
            for workers in dict.fromkeys([1, k] if repeat % 2 == 0 else [k, 1]):
                record = in_scheduler(shape, k, rows, workers)
                records.append(dict(record, repeat=repeat))
        for workers in sorted({1, k}):
            runs = [r for r in records if r["shape"] == shape and r["workers"] == workers]
            wall = median(r["wall_s"] for r in runs)
            first = [r["first_result_s"] for r in runs if r["first_result_s"] is not None]
            first_label = f"{median(first):.3f}s" if first else "unavailable"
            overruns = sum(r["step_overruns"] for r in runs)
            steps = sum(r["step_count"] for r in runs)
            print(f"  {shape:9s} workers={workers} median wall={wall:.3f}s"
                  f" first result={first_label} overruns={overruns}/{steps}")
    if args.json:
        args.json.write_text(json.dumps({
            "python": sys.version, "free_threaded_build": ft_build, "gil_enabled": gil,
            "k": k, "rows": rows, "repeats": args.repeats,
            "metric_note": "Worker step completion with productive output; not UI presentation or interaction latency.",
            "runs": records,
        }, indent=2) + "\n")


if __name__ == "__main__":
    main()
