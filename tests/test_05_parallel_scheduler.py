"""
The parallel scheduler (Scheduler(workers=N)): modules that are not directly
connected run their steps concurrently on worker threads.

Checks that it is safe (connected modules never run at the same time), that it
actually runs modules concurrently, and that results stay exact.
"""

from __future__ import annotations

import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

from . import ProgressiveTest
from .test_04_progressive_guarantees import TestPrefixConsistency
from progressivis import Max, Min, RandomPTable, Scheduler, Sink, Var
from progressivis.core import aio
from progressivis.core.module import Module

WORKERS = 4
ROWS = 50_000


class _OrderedScheduler(Scheduler):
    """Exercise the real run loop with one explicitly ordered sweep."""

    async def _next_module(self, *args: Any, **kwargs: Any) -> Any:
        for i, module in enumerate(self._run_list):
            self._run_index = i + 1
            yield module

    def _schedule(self, module: Any) -> bool:
        self._run_number += 1
        return True

    def _consider_module(self, module: Any) -> bool:
        return True

    def no_more_data(self) -> bool:
        return False


def _task(name: str, order: int, run: Any, upstream: Any = None) -> Any:
    module = SimpleNamespace(
        name=name, order=order,
        run=lambda run_number, quantum=None: run(run_number),
        params=SimpleNamespace(quantum=0.5),
        start_run=AsyncMock(), after_run=AsyncMock(),
        _start_run=[], _after_run=[],
        input_slot_values=lambda: (
            [] if upstream is None else [SimpleNamespace(output_module=upstream)]
        ),
        output_slot_values=lambda: [],
    )
    return module


class TestParallelOrdering(ProgressiveTest):
    def _run_tasks(self, modules: Any, tick: Any = None) -> None:
        s = _OrderedScheduler(workers=2)
        s._stopped = False
        s.shortcut_evt = aio.Event()
        s._run_list = modules
        if tick is not None:
            s.on_tick(tick)
        with ThreadPoolExecutor(max_workers=2) as executor:
            s._executor = executor
            aio.run(s._run_loop())

    def test_ready_consumer_does_not_wait_for_unrelated_worker(self) -> None:
        c_started, b_finished = threading.Event(), threading.Event()
        observed = []
        a = _task("a", 0, lambda _: c_started.wait(1))
        b = _task("b", 1, lambda _: b_finished.set(), a)

        def run_c(_: int) -> None:
            c_started.set()
            observed.append(b_finished.wait(1))

        c = _task("c", 2, run_c)
        self._run_tasks([a, b, c])
        self.assertEqual(observed, [True], "B waited for unrelated C to finish")

    def test_tick_callbacks_wait_for_workers(self) -> None:
        started, finished, callback = (threading.Event() for _ in range(3))
        observed = []
        a = _task("a", 0, lambda _: started.wait(1))

        def run_b(_: int) -> None:
            started.set()
            # A callback on the old scheduler releases this worker early.
            # A safe scheduler waits for its bounded independent completion.
            callback.wait(0.2)
            finished.set()

        def tick(*_: Any) -> None:
            observed.append(finished.is_set())
            callback.set()

        self._run_tasks([a, _task("b", 1, run_b)], tick)
        self.assertTrue(observed)
        self.assertTrue(all(observed), "tick callback overlapped a worker")

    def test_lifecycle_callbacks_wait_for_workers(self) -> None:
        finished, callback = threading.Event(), threading.Event()
        observed = []

        def run_a(_: int) -> None:
            callback.wait(0.2)
            finished.set()

        async def check(*_: Any) -> None:
            observed.append(finished.is_set())
            callback.set()

        a = _task("a", 0, run_a)
        b = _task("b", 1, lambda _: None)
        b._start_run = [check]
        b._after_run = [check]
        b.start_run = check
        b.after_run = check
        self._run_tasks([a, b])
        self.assertEqual(observed, [True, True])

    def test_graph_update_waits_for_workers_before_teardown(self) -> None:
        finished = threading.Event()
        observed = []

        class EditingScheduler(_OrderedScheduler):
            async def _next_module(self, *args: Any, **kwargs: Any) -> Any:
                self._run_index = 1
                yield self._run_list[0]
                self._enter_cnt = 0
                self._stopped = False
                async for module in Scheduler._next_module(self, *args, **kwargs):
                    yield module

            def _update_modules(self) -> None:
                observed.append(finished.is_set())
                self.dataflow = None
                self._run_list = []

        s = EditingScheduler(workers=2)
        s._stopped = False
        s.shortcut_evt = aio.Event()

        def run(_: int) -> None:
            # Yield the worker so the old implementation can update the graph
            # before completion. The assertion concerns ordering, not duration.
            finished.wait(0.2)
            finished.set()

        s._run_list = [_task("a", 0, run), _task("b", 1, lambda _: None)]
        with ThreadPoolExecutor(max_workers=2) as executor:
            s._executor = executor
            aio.run(s._run_loop())
        self.assertEqual(observed, [True])

    def test_input_waits_for_the_running_step(self) -> None:
        # Variable.from_input awaits for_input() and then changes the module:
        # for_input() must not return while that module runs on a worker.
        started, release, finished = (threading.Event() for _ in range(3))
        observed = []

        def run_a(_: int) -> None:
            started.set()
            release.wait(0.5)  # an unsafe for_input() returns meanwhile
            finished.set()

        a = _task("a", 0, run_a)
        s = _OrderedScheduler(workers=2)
        s._stopped = False
        s.shortcut_evt = aio.Event()
        s._run_list = [a]

        async def give_input() -> None:
            while not started.is_set():
                await aio.sleep(0.001)
            await s.for_input(a)
            observed.append(finished.is_set())
            release.set()

        async def main() -> None:
            s._hibernate_cond = aio.Condition()
            await aio.gather(s._run_loop(), give_input())

        with ThreadPoolExecutor(max_workers=2) as executor:
            s._executor = executor
            aio.run(main())
        self.assertEqual(observed, [True], "input changed a module during its step")


def _pipelines(s: Scheduler, k: int) -> List[Tuple[RandomPTable, Any, Any, Any]]:
    "k independent pipelines: RandomPTable -> {Min, Max, Var}"
    out = []
    for _ in range(k):
        src = RandomPTable(3, rows=ROWS, scheduler=s)
        mods = []
        for cls in (Min, Max, Var):
            mod = cls(scheduler=s)
            mod.input.table = src.output.result
            sink = Sink(scheduler=s)
            sink.input.inp = mod.output.result
            mods.append(mod)
        out.append((src, mods[0], mods[1], mods[2]))
    return out


def _connected(modules: List[Module]) -> set[Tuple[str, str]]:
    "Pairs of modules directly connected by a slot (independent of the scheduler)"
    pairs = set()
    for m in modules:
        for slot in m.input_slot_values():
            pairs.add((m.name, slot.output_module.name))
            pairs.add((slot.output_module.name, m.name))
    return pairs


class TestParallelScheduler(ProgressiveTest):
    def test_connected_modules_never_overlap(self) -> None:
        intervals: Dict[str, List[Tuple[float, float]]] = {}
        lock = threading.Lock()
        original = Module.run

        def timed_run(
            module: Module, run_number: int, quantum: Optional[float] = None
        ) -> None:
            start = time.perf_counter()
            original(module, run_number, quantum)
            with lock:
                intervals.setdefault(module.name, []).append(
                    (start, time.perf_counter())
                )

        s = Scheduler(workers=WORKERS)
        _pipelines(s, WORKERS)
        setattr(Module, "run", timed_run)
        try:
            aio.run(s.start())
        finally:
            setattr(Module, "run", original)

        def overlap(a: str, b: str) -> bool:
            return any(
                s1 < e2 and s2 < e1
                for (s1, e1) in intervals.get(a, [])
                for (s2, e2) in intervals.get(b, [])
            )

        modules = list(s.modules().values())
        connected = _connected(modules)
        self.assertEqual(
            [pair for pair in connected if overlap(*pair)], [],
            "directly connected modules ran at the same time",
        )
        names = [m.name for m in modules]
        concurrent = [
            (a, b)
            for i, a in enumerate(names)
            for b in names[i + 1:]
            if (a, b) not in connected and overlap(a, b)
        ]
        self.assertTrue(concurrent, "no modules ran concurrently")

    def test_input_state_is_read_in_the_event_loop(self) -> None:
        # Interactive selection is changed by for_input()/shortcut_manager in
        # the event loop; workers must not read or reset it.
        threads = set()
        original = Scheduler.fix_quantum

        def recording(sched: Scheduler, module: Module, quantum: float) -> float:
            threads.add(threading.get_ident())
            return original(sched, module, quantum)

        s = Scheduler(workers=WORKERS)
        _pipelines(s, 2)
        setattr(Scheduler, "fix_quantum", recording)
        try:
            aio.run(s.start())
        finally:
            setattr(Scheduler, "fix_quantum", original)
        self.assertEqual(threads, {threading.get_ident()})

    def test_blas_threads_limited_while_running(self) -> None:
        import sklearn.utils  # type: ignore  # noqa: F401  loads OpenMP; numpy may load a BLAS
        from threadpoolctl import threadpool_info  # type: ignore

        before = [pool["num_threads"] for pool in threadpool_info()]
        if not before:
            self.skipTest("no BLAS/OpenMP thread pool detected")
        during: List[List[int]] = []
        s = Scheduler(workers=WORKERS, blas_threads=1)
        _pipelines(s, 1)
        s.on_tick(lambda *_: during.append([p["num_threads"] for p in threadpool_info()]))
        aio.run(s.start())
        self.assertTrue(during)
        self.assertTrue(all(n == 1 for counts in during for n in counts), during[0])
        self.assertEqual([pool["num_threads"] for pool in threadpool_info()], before)

    def test_blas_threads_default(self) -> None:
        cores = getattr(os, "process_cpu_count", os.cpu_count)() or 1
        with patch.dict(os.environ):
            os.environ.pop("PROGRESSIVIS_BLAS_THREADS", None)
            self.assertEqual(Scheduler(workers=4)._blas_threads, max(1, cores // 4))
            self.assertEqual(Scheduler(workers=4, blas_threads=0)._blas_threads, 0)
            os.environ["PROGRESSIVIS_BLAS_THREADS"] = "3"
            self.assertEqual(Scheduler(workers=4)._blas_threads, 3)

    def test_results_are_exact(self) -> None:
        s = Scheduler(workers=WORKERS)
        pipelines = _pipelines(s, WORKERS)
        aio.run(s.start())
        for src, min_, max_, var in pipelines:
            assert src.result is not None
            data = src.result.to_array()
            for mod, expected in (
                (min_, data.min(axis=0)),
                (max_, data.max(axis=0)),
                (var, data.var(axis=0)),
            ):
                result: Any = mod.result
                got = np.array([result[c] for c in src.result.columns])
                self.assertTrue(np.allclose(got, expected), type(mod).__name__)


class TestPrefixConsistencyParallel(TestPrefixConsistency):
    "The per-step guarantees, with a parallel scheduler"

    @property
    def scheduler(self) -> Scheduler:
        if self._scheduler is None:
            self._scheduler = Scheduler(workers=WORKERS)
        return self._scheduler


if __name__ == "__main__":
    ProgressiveTest.main()
