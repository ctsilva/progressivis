"""Failure, cancellation and pause behavior with real worker threads."""
from __future__ import annotations

import asyncio
import threading
from typing import Any, Callable

from . import ProgressiveTest
from progressivis import Scheduler, ProgressiveError
from progressivis.core.module import Module, ReturnRunStep
from progressivis.core.scheduler import SchedulerRunError


class _Step(Module):
    def __init__(self, action: Callable[[], None], **kwargs: Any):
        super().__init__(**kwargs)
        self.action = action
        self.calls = 0
        self.endings = 0
        self.active = False

    def is_ready(self) -> bool:
        return self.state in (self.state_created, self.state_ready)

    def predict_step_size(self, duration: float) -> int:
        return 1

    def run_step(self, run_number: int, step_size: int, quantum: float) -> ReturnRunStep:
        self.calls += 1
        self.active = True
        try:
            self.action()
        finally:
            self.active = False
        return self._return_run_step(self.state_zombie, steps_run=1)

    async def ending(self) -> None:
        assert not self.active, "ending raced the worker"
        self.endings += 1
        await super().ending()


async def _started(event: threading.Event) -> None:
    async def wait() -> None:
        while not event.is_set():
            await asyncio.sleep(0.001)
    await asyncio.wait_for(wait(), 2)


class TestSchedulerLifecycle(ProgressiveTest):
    def assert_stopped(self, scheduler: Scheduler) -> None:
        self.assertFalse(scheduler.is_running())
        self.assertTrue(scheduler.is_stopped())
        self.assertFalse(scheduler._task)
        self.assertIsNone(scheduler._executor)
        self.assertFalse(scheduler._worker_futures)

    def test_cancel_joins_workers_without_blocking_loop(self) -> None:
        async def exercise(fail: bool) -> None:
            s = Scheduler(workers=2)
            started, release = threading.Event(), threading.Event()
            released = []
            companion_ended = asyncio.Event()

            def work() -> None:
                started.set()
                released.append(release.wait(1))
                if fail:
                    raise ValueError("late worker failure")

            module = _Step(work, scheduler=s)

            async def companion() -> None:
                try:
                    await asyncio.Event().wait()
                finally:
                    companion_ended.set()

            task = asyncio.create_task(s.start(coros=[companion()]))
            await _started(started)
            task.cancel()
            asyncio.get_running_loop().call_later(0.01, task.cancel)
            asyncio.get_running_loop().call_later(0.03, release.set)
            with self.assertRaises(asyncio.CancelledError) as caught:
                await task
            self.assertEqual(released, [True], "cleanup blocked the event loop")
            self.assertTrue(companion_ended.is_set())
            self.assertEqual(module.endings, 1)
            self.assert_stopped(s)
            if fail:
                self.assertIsInstance(caught.exception.__cause__, SchedulerRunError)
                self.assertIn("late worker failure", str(caught.exception.__cause__))
            with self.assertRaises(ProgressiveError):
                await s.start()

        for fail in (False, True):
            with self.subTest(fail=fail):
                asyncio.run(exercise(fail))

    def test_multiple_worker_errors_are_retained(self) -> None:
        s = Scheduler(workers=2)
        barrier = threading.Barrier(2)

        def fail(message: str) -> None:
            barrier.wait(timeout=2)
            raise ValueError(message)

        a = _Step(lambda: fail("worker-a"), name="a", scheduler=s)
        b = _Step(lambda: fail("worker-b"), name="b", scheduler=s)
        c = _Step(lambda: None, name="c", scheduler=s)
        with self.assertRaises(SchedulerRunError) as caught:
            asyncio.run(s.start())
        self.assertEqual(len(caught.exception.errors), 2)
        self.assertIn("worker-a", str(caught.exception))
        self.assertIn("worker-b", str(caught.exception))
        self.assertEqual(c.calls, 0)
        self.assertEqual([m.endings for m in (a, b, c)], [1, 1, 1])
        self.assert_stopped(s)

    def test_companion_failure_joins_worker(self) -> None:
        async def exercise() -> None:
            s = Scheduler(workers=2)
            started, release = threading.Event(), threading.Event()
            released = []

            def work() -> None:
                started.set()
                released.append(release.wait(1))

            module = _Step(work, scheduler=s)

            async def companion() -> None:
                await _started(started)
                asyncio.get_running_loop().call_later(0.03, release.set)
                raise ValueError("companion failure")

            with self.assertRaisesRegex(ValueError, "companion failure"):
                await s.start(coros=[companion()])
            self.assertEqual(released, [True])
            self.assertEqual(module.endings, 1)
            self.assert_stopped(s)

        asyncio.run(exercise())

    def test_normal_completion_cancels_background_companion(self) -> None:
        async def exercise() -> None:
            s = Scheduler(workers=2)
            module = _Step(lambda: None, scheduler=s)
            ended = asyncio.Event()

            async def companion() -> None:
                try:
                    await asyncio.Event().wait()
                finally:
                    ended.set()

            await asyncio.wait_for(s.start(coros=[companion()]), 2)
            self.assertTrue(ended.is_set())
            self.assertEqual(module.endings, 1)
            self.assertFalse(s._aborted)
            self.assert_stopped(s)

        asyncio.run(exercise())

    def test_stop_discards_unstarted_work_and_can_resume(self) -> None:
        async def exercise() -> None:
            s = Scheduler(workers=2)
            started, release = threading.Event(), threading.Event()

            def work() -> None:
                started.set()
                assert release.wait(1)

            a = _Step(work, name="a", scheduler=s)
            b = _Step(work, name="b", scheduler=s)
            c = _Step(lambda: None, name="c", scheduler=s)
            task = asyncio.create_task(s.start())
            await _started(started)
            await s.stop()
            release.set()
            await task
            self.assertEqual(c.calls, 0)
            self.assertFalse(s._aborted)
            self.assert_stopped(s)
            await s.start()
            self.assertEqual(c.calls, 1)
            self.assertEqual([m.endings for m in (a, b, c)], [1, 1, 1])

        asyncio.run(exercise())

    def test_startup_failure_restores_flags(self) -> None:
        s = Scheduler(workers=2)
        module = _Step(lambda: None, scheduler=s)

        def fail() -> None:
            raise ValueError("startup failure")

        s._before_run = fail  # type: ignore[method-assign]
        with self.assertRaisesRegex(ValueError, "startup failure"):
            asyncio.run(s.start())
        self.assertEqual(module.endings, 1)
        self.assert_stopped(s)

    def test_teardown_failure_does_not_skip_other_modules(self) -> None:
        s = Scheduler(workers=2)

        def fail() -> None:
            raise ValueError("worker failure")

        a = _Step(fail, scheduler=s)
        b = _Step(lambda: None, scheduler=s)

        async def bad_ending() -> None:
            b.endings += 1
            raise ValueError("ending failure")

        b.ending = bad_ending  # type: ignore[method-assign]
        with self.assertRaises(SchedulerRunError) as caught:
            asyncio.run(s.start())
        self.assertIn("worker failure", str(caught.exception))
        self.assertIn("ending failure", str(caught.exception))
        self.assertEqual([a.endings, b.endings], [1, 1])
        self.assert_stopped(s)

    def test_cancel_during_ending_does_not_repeat_or_interrupt_it(self) -> None:
        async def exercise() -> None:
            s = Scheduler(workers=2)
            module = _Step(lambda: None, scheduler=s)
            entered, release = asyncio.Event(), asyncio.Event()
            finished = []

            async def ending() -> None:
                module.endings += 1
                entered.set()
                await release.wait()
                finished.append(True)

            module.ending = ending  # type: ignore[method-assign]
            task = asyncio.create_task(s.start())
            await asyncio.wait_for(entered.wait(), 2)
            task.cancel()
            asyncio.get_running_loop().call_later(0.01, task.cancel)
            asyncio.get_running_loop().call_later(0.03, release.set)
            with self.assertRaises(asyncio.CancelledError):
                await task
            self.assertEqual(module.endings, 1)
            self.assertEqual(finished, [True])
            self.assert_stopped(s)

        asyncio.run(exercise())

    def test_final_hook_failure_is_reported_and_cleans_paused_modules(self) -> None:
        async def exercise() -> None:
            s = Scheduler(workers=1)
            module = _Step(lambda: None, scheduler=s)

            async def pause(*_: Any) -> None:
                await s.stop()

            def fail() -> None:
                raise ValueError("final hook failure")

            module.on_before_run(pause)
            s._after_run = fail  # type: ignore[method-assign]
            with self.assertRaisesRegex(ValueError, "final hook failure"):
                await s.start()
            self.assertEqual(module.calls, 0)
            self.assertEqual(module.endings, 1)
            self.assert_stopped(s)

        asyncio.run(exercise())
