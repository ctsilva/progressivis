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

Usage: python scripts/bench_parallel.py [K] [ROWS]
"""

from __future__ import annotations

import sys
import sysconfig
import threading
import time
import warnings

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
    s = Scheduler()
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
    k = int(sys.argv[1]) if len(sys.argv) > 1 else 4
    rows = int(sys.argv[2]) if len(sys.argv) > 2 else 2_000_000
    gil = getattr(sys, "_is_gil_enabled", lambda: True)()
    ft_build = bool(sysconfig.get_config_var("Py_GIL_DISABLED"))
    print(f"python {sys.version.split()[0]}  free-threaded build={ft_build}  "
          f"GIL enabled={gil}  K={k}  rows={rows:,}")
    if ft_build and gil:
        print("warning: GIL re-enabled by an extension; rerun with PYTHON_GIL=0")
    run_one(rows // 10)  # warm-up: imports, first-call costs
    ts, cs = serial(k, rows)
    tp, cp = threaded(k, rows)
    print(f"serial   wall {ts:7.2f}s  cpu {cs:7.2f}s  (cpu/wall {cs / ts:4.2f})")
    print(f"threaded wall {tp:7.2f}s  cpu {cp:7.2f}s  (cpu/wall {cp / tp:4.2f})"
          f"   speedup {ts / tp:4.2f}x  (ideal {k}x)")


if __name__ == "__main__":
    main()
