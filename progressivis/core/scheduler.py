"""
The Scheduler class runs progressive modules in an asynchronous process.
"""

from __future__ import annotations

import logging
import functools
from asyncio import CancelledError, shield
from concurrent.futures import ThreadPoolExecutor
from weakref import WeakKeyDictionary
import os
import inspect as ins  # https://github.com/python/cpython/issues/122858
from timeit import default_timer
import traceback
from io import StringIO, SEEK_END
from .dataflow import Dataflow
from . import aio
from ..utils.errors import ProgressiveError

from typing import (
    Optional,
    Dict,
    List,
    Sequence,
    Any,
    Callable,
    Set,
    Coroutine,
    Union,
    AsyncGenerator,
    Awaitable,
    Tuple,
    TYPE_CHECKING,
)

class FakeProfile:
    def enable(self) -> None:
        pass
    def disable(self) -> None:
        pass


PROFILER = FakeProfile

if os.getenv("PROGRESSIVIS_PROFILE"):
    import cProfile
    PROFILER = cProfile.Profile  # type: ignore

logger = logging.getLogger(__name__)

__all__ = ["Scheduler", "SchedulerRunError"]


class SchedulerRunError(RuntimeError):
    "Multiple failures from one run, including worker and cleanup failures."

    def __init__(self, errors: Sequence[BaseException]):
        self.errors = tuple(errors)
        super().__init__("Scheduler failures: " + "; ".join(str(e) for e in errors))

KEEP_RUNNING = 5
SHORTCUT_TIME: float = 1.5

if TYPE_CHECKING:
    from progressivis.core.module import Module
    from progressivis.core.slot import Slot

    Dependencies = Dict[str, Dict[str, Slot]]

TickCb = Callable[["Scheduler", int], None]
TickCoro = Callable[["Scheduler", int], Coroutine[Any, Any, Any]]
TickProc = Union[TickCb, TickCoro]
ChangeProc = Callable[
    ["Scheduler", Set["Module"], Set["Module"]], Coroutine[Any, Any, None]
]
Order = List[str]
Reachability = Dict[str, List[str]]


class CallbackList(Dict[TickProc, int]):
    async def fire(self, scheduler: Scheduler, run_number: int) -> bool:
        ret = False
        for proc, count in list(self.items()):
            try:
                if count > 0:
                    count -= 1
                    self[proc] = count
                    continue
                if count == 0:
                    del self[proc]
                res = proc(scheduler, run_number)
                if ins.iscoroutine(res):
                    await res
                ret = True
            except Exception as exc:
                logger.warning(exc)
        return ret


class Scheduler:
    """
    A Scheduler runs progressive modules
    """

    # pylint: disable=too-many-public-methods,too-many-instance-attributes
    default: "Scheduler"
    """Default scheduler, used implicitly when not specified in
    ProgressiVis methods"""

    _last_id: int = 0
    @classmethod
    def or_default(cls, scheduler: Optional["Scheduler"]) -> "Scheduler":
        "Return the specified scheduler of, in None, the default one."
        return scheduler or cls.default

    def __init__(
        self,
        interaction_latency: int = 1,
        workers: Optional[int] = None,
        blas_threads: Optional[int] = None,
    ):
        """
        Args:
            interaction_latency: target latency (s) in interactive mode
            workers: number of modules that can run at the same time, on
                separate threads (default: env PROGRESSIVIS_WORKERS, else 1).
                With more than one, independent modules (no direct slot
                connection between them) run their steps concurrently.
            blas_threads: with more than one worker, threads that BLAS and
                OpenMP libraries (numpy, scipy, scikit-learn) may use while
                the scheduler runs, so that the workers do not oversubscribe
                the cores (default: env PROGRESSIVIS_BLAS_THREADS, else
                cores // workers; 0 leaves the libraries unchanged). The limit
                is process-wide and restored when the run ends.
        """
        if workers is None:
            workers = int(os.environ.get("PROGRESSIVIS_WORKERS", "1"))
        self._workers: int = max(1, workers)
        if blas_threads is None:
            env = os.environ.get("PROGRESSIVIS_BLAS_THREADS")
            if env is not None:
                blas_threads = int(env)
            else:
                cores = getattr(os, "process_cpu_count", os.cpu_count)() or 1
                blas_threads = max(1, cores // self._workers)
        self._blas_threads: int = max(0, blas_threads)
        self._thread_limits: Any = None  # threadpoolctl limits while running
        self._executor: Optional[ThreadPoolExecutor] = None
        self._worker_futures: Set[aio.Future[None]] = set()
        # Module name -> future of the step it runs on a worker thread
        self._active_steps: Dict[str, aio.Future[None]] = {}
        self._run_errors: List[BaseException] = []
        self._aborted = False
        self._ending_tasks: WeakKeyDictionary[Module, aio.Task[None]] = WeakKeyDictionary()
        if interaction_latency <= 0:
            raise ProgressiveError(
                "Invalid interaction_latency, "
                "should be strictly positive: %s" % interaction_latency
            )

        # same as clear below
        Scheduler._last_id += 1
        self._name: int = Scheduler._last_id
        self._modules: Dict[str, Module] = {}
        self._dependencies: Dependencies
        self._running: bool = False
        self._stopped: bool = True
        self._runorder: Order = []
        self._added_modules: Set[Module] = set()
        self._deleted_modules: Set[Module] = set()
        self._start: float = 0
        self._step_once = False
        self._run_number = 0
        self._tick_procs = CallbackList()
        self._idle_procs = CallbackList()
        self._loop_procs = CallbackList()
        self._change_procs: Set[ChangeProc] = set()
        self.version = 0
        self._run_list: List[Module] = []
        self._run_index = 0
        self._module_selection: Optional[Set[str]] = None
        self._selection_target_time: float = -1
        self.interaction_latency = interaction_latency
        self._reachability: Reachability = {}
        self._start_inter: float = 0
        self._hibernate_cond: aio.Condition
        self._keep_running: float = KEEP_RUNNING
        self.dataflow: Optional[Dataflow] = Dataflow(self)
        self.module_iterator = None
        self._enter_cnt = 1
        self._lock: aio.Lock
        self._task = False
        self.shortcut_evt: aio.Event
        self.coros: List[Coroutine[Any, Any, Any]] = []
        self._multiple_slots_name_generator = 1
        self.profiler = PROFILER()

    def new_run_number(self) -> int:
        self._run_number += 1
        return self._run_number

    # Context manager for adding new modules
    def __enter__(self) -> Dataflow:
        if self.dataflow is None:
            self.dataflow = Dataflow(self)
            self.dataflow.multiple_slots_name_generator = (
                self._multiple_slots_name_generator
            )
            self._enter_cnt = 1
        else:
            self._enter_cnt += 1
        return self.dataflow

    def __exit__(self, exc_type: Any, exc_value: Any, tb: Any) -> None:
        self._enter_cnt -= 1
        if exc_type is not None:
            logger.error("Aborting Dataflow with exception %s", exc_type)
            print(f"Aborting Dataflow with exception {exc_type}")
            traceback.print_tb(tb)
            if self._enter_cnt == 0:
                if self.dataflow is not None:
                    self.dataflow.aborted()
                self.dataflow = None
        if self._enter_cnt > 0 or self.dataflow is None:
            return
        errors = self.dataflow.validate()
        if errors:
            logger.error("Dataflow has errors %s", errors)
            if self.dataflow is not None:
                self.dataflow.aborted()
                self.dataflow = None
            raise ProgressiveError(f"Invalid dataflow: {errors}")

    def in_context_manager(self) -> bool:
        return self._enter_cnt > 0

    @property
    def name(self) -> str:
        "Return the scheduler id"
        return str(self._name)

    def timer(self) -> float:
        "Return the scheduler timer."
        if self._start == 0:
            self._start = default_timer()
            return 0
        return default_timer() - self._start

    def run_queue_length(self) -> int:
        "Return the length of the run queue"
        return len(self._run_list)

    def to_json(self, short: bool = True) -> Dict[str, Any]:
        "Return a dictionary describing the scheduler"
        msg: Dict[str, Any] = {}
        mods = {}
        for name, module in self.modules().items():
            mods[name] = module.to_json(short=short)
        modules = sorted(mods.values(), key=functools.cmp_to_key(self._module_order))
        msg["modules"] = modules
        msg["is_running"] = self.is_running()
        msg["is_terminated"] = self.is_terminated()
        msg["run_number"] = self.run_number()
        msg["status"] = "success"
        return msg

    def _repr_html_(self) -> str:
        html_head = "<div type='schedule'>"
        html_head = """
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>"""
        html_end = "</div>"
        html_head += f"""
<p><b>Scheduler</b> {hex(id(self))}
        <b>{"running" if self.is_running() else "stopped"}</b>,
        <b>modules:</b> {len(self)},
        <b>run number:</b> {self.run_number()}
</p>"""
        html_head += """
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th>Id</th><th>Class</th><th>State</th><th>Last Update</th><th>Order</th>
    </tr>
  </thead>
  <tbody>"""
        columns = ["id", "classname", "state", "last_update", "order"]
        for mod in self._run_list:
            values = mod.to_json(short=True)
            html_head += "<tr>"
            html_head += "".join(
                ["<td>%s</td>" % (values[column]) for column in columns]
            )
        html_end = "</tbody></table>"
        return html_head + html_end

    @staticmethod
    def set_default() -> None:
        "Set the default scheduler."
        if not isinstance(Scheduler.default, Scheduler):
            Scheduler.default = Scheduler()

    def _before_run(self) -> None:
        logger.debug("Before run %d", self._run_number)

    def _after_run(self) -> None:
        pass

    async def start_impl(
        self,
        tick_proc: Optional[TickProc] = None,
        idle_proc: Optional[TickProc] = None,
        coros: Sequence[Coroutine[Any, Any, Any]] = (),
    ) -> None:
        async with self._lock:
            if self._aborted:
                raise ProgressiveError("Aborted dataflow cannot restart; create a fresh scheduler")
            if self._task:
                raise ProgressiveError(
                    "Trying to start scheduler task inside scheduler task"
                )
            print("Starting scheduler")
            self._task = True
        self.coros = list(coros)
        if tick_proc:
            self.on_tick(tick_proc)
        if idle_proc:
            self.on_idle(idle_proc)
        await self.run()

    async def start(
        self,
        tick_proc: Optional[TickProc] = None,
        idle_proc: Optional[TickProc] = None,
        coros: Sequence[Coroutine[Any, Any, Any]] = (),
        persist: bool = False,
    ) -> None:
        """Run the dataflow and supervise ``coros`` for this run's lifetime.

        Unfinished companion coroutines are cancelled and awaited when the
        dataflow finishes or pauses. Failure/cancellation joins running workers
        and ends modules before returning; an aborted scheduler cannot restart.
        Cancellation cannot preempt a worker, so cleanup has no time bound.
        """
        from ..storage import init_temp_dir_if, cleanup_temp_dir, temp_dir

        if self._task:
            return
        self.shortcut_evt = aio.Event()
        self._hibernate_cond = aio.Condition()
        self._lock = aio.Lock()
        itd_flag = False
        if not persist:
            return await self.start_impl(tick_proc, idle_proc, coros)
        try:
            itd_flag = init_temp_dir_if()
            if itd_flag:
                print("Init TEMP_DIR in start()", temp_dir())
            return await self.start_impl(tick_proc, idle_proc, coros)
        finally:
            if itd_flag:
                cleanup_temp_dir()

    def task_start(self, *args: Any, **kwargs: Any) -> aio.Task[Any]:
        return aio.create_task(self.start(*args, **kwargs))

    def _step_proc(self, s: Scheduler, run_number: int) -> None:
        # pylint: disable=unused-argument
        self.task_stop()

    async def step(self) -> None:
        "Start the scheduler for on step."
        await self.start(tick_proc=self._step_proc)

    def on_tick(self, proc: TickProc, delay: int = -1) -> TickProc:
        "Set a procedure to call at each tick."
        assert callable(proc)
        self._tick_procs[proc] = delay
        return proc

    def remove_tick(self, proc: TickProc) -> None:
        "Remove a tick callback"
        self._tick_procs.pop(proc, None)

    def on_tick_once(self, proc: TickProc) -> TickProc:
        """
        Add a oneshot function that will be run at the next scheduler tick.
        This is especially useful for setting up module connections.
        """
        self.on_tick(proc, 1)
        return proc

    def remove_tick_once(self, proc: TickProc) -> None:
        "Remove a tick once callback"
        self.remove_tick(proc)

    def on_idle(self, proc: TickProc, delay: int = -1) -> TickProc:
        "Set a procedure that will be called when there is nothing else to do."
        assert callable(proc)
        self._idle_procs[proc] = delay
        return proc

    def remove_idle(self, idle_proc: TickProc) -> None:
        "Remove an idle callback."
        self._idle_procs.pop(idle_proc, None)

    def on_loop(self, proc: TickProc, delay: int = -1) -> TickProc:
        assert callable(proc)
        self._loop_procs[proc] = delay
        return proc

    def remove_loop(self, idle_proc: TickProc) -> None:
        "Remove an idle callback."
        self._loop_procs.pop(idle_proc, None)

    def on_change(self, proc: ChangeProc) -> ChangeProc:
        assert callable(proc)
        self._change_procs.add(proc)
        return proc

    def remove_change(self, proc: ChangeProc) -> None:
        self._change_procs.remove(proc)

    async def run(self) -> None:
        "Run until completion, pause, failure or cancellation; always join workers."
        global KEEP_RUNNING
        if self._aborted:
            raise ProgressiveError("Aborted dataflow cannot restart; create a fresh scheduler")
        runners: List[aio.Task[Any]] = []
        primary: Optional[BaseException] = None
        self._run_errors = []
        try:
            self.commit()
            self._stopped = False
            self._running = True
            self._start = default_timer()
            self._before_run()
            if self._workers > 1:
                self._executor = ThreadPoolExecutor(
                    max_workers=self._workers, thread_name_prefix=f"progressivis-{self.name}"
                )
                if self._blas_threads:
                    from threadpoolctl import threadpool_limits  # type: ignore

                    self._thread_limits = threadpool_limits(limits=self._blas_threads)
            run_loop = aio.create_task(self._run_loop())
            runners = [run_loop, aio.create_task(self.shortcut_manager())]
            runners.extend(aio.create_task(coro) for coro in self.coros)
            KEEP_RUNNING = min(50, len(self._run_list) * 3)
            self._keep_running = KEEP_RUNNING
            pending = set(runners)
            while pending:
                done, pending = await aio.wait(pending, return_when=aio.FIRST_COMPLETED)
                for task in runners:
                    if task in done:
                        task.result()  # surface companion errors as well as worker errors
                if run_loop in done:
                    break
        except BaseException as exc:
            primary = exc
            self._aborted = True
            self._stopped = True
            if not isinstance(exc, CancelledError):
                self._record_error(exc)
        finally:
            # Shield a separate cleanup task: repeated cancel() calls must not
            # abandon workers or interrupt a module's ending hook.
            cleanup = aio.create_task(self._finish_run(runners))
            while True:
                try:
                    await shield(cleanup)
                    break
                except CancelledError as exc:
                    primary = exc
                    self._aborted = True
                except BaseException as exc:
                    self._record_error(exc)
                    break
        if isinstance(primary, CancelledError):
            if self._run_errors:
                raise primary from SchedulerRunError(self._run_errors)
            raise primary
        if len(self._run_errors) == 1:
            raise self._run_errors[0]
        if self._run_errors:
            raise SchedulerRunError(self._run_errors) from primary

    def _record_error(self, exc: BaseException) -> None:
        if not any(exc is old for old in self._run_errors):
            self._run_errors.append(exc)
        self._aborted = True
        self._stopped = True

    def _worker_done(self, future: aio.Future[None]) -> None:
        self._worker_futures.discard(future)
        if not future.cancelled():
            exc = future.exception()
            if exc is not None:
                self._record_error(exc)

    async def _end_module(self, module: Module) -> None:
        # Retain the task even if its waiter is cancelled: cleanup awaits the
        # same invocation rather than calling ending() twice.
        if module not in self._ending_tasks:
            self._ending_tasks[module] = aio.create_task(module.ending())
        await shield(self._ending_tasks[module])

    async def _finish_run(self, runners: Sequence[aio.Task[Any]]) -> None:
        self._stopped = True
        try:
            if not runners:
                # Setup failed before companion coroutines became tasks.
                for coro in self.coros:
                    coro.close()
            for task in runners:
                if not task.done():
                    task.cancel()
            results = await aio.gather(*runners, return_exceptions=True)
            for result in results:
                if isinstance(result, BaseException) and not isinstance(result, CancelledError):
                    self._record_error(result)
            # asyncio.wait in the run loop never cancels executor futures.
            # Joining asynchronously keeps input/timers and worker dependencies
            # on this event loop alive until the threads have actually finished.
            if self._worker_futures:
                await aio.gather(*self._worker_futures, return_exceptions=True)
            self._active_steps.clear()
            if self._executor is not None:
                self._executor.shutdown(wait=True)
                self._executor = None
            if self._thread_limits is not None:
                self._thread_limits.restore_original_limits()
                self._thread_limits = None
            aborted_before_hooks = self._aborted
            if aborted_before_hooks:
                await self._end_all_modules()
            elif self._ending_tasks:
                for result in await aio.gather(*self._ending_tasks.values(), return_exceptions=True):
                    if isinstance(result, BaseException):
                        self._record_error(result)
            for hook in (self._after_run, self.done):
                try:
                    hook()
                except BaseException as exc:
                    self._record_error(exc)
            # A final hook or cancellation during normal cleanup can turn a
            # resumable pause into an abort. It still needs module teardown.
            if self._aborted and not aborted_before_hooks:
                await self._end_all_modules()
        finally:
            self._running = False
            self._stopped = True
            self._task = False

    async def _end_all_modules(self) -> None:
        modules = set(self._modules.values()) | self._deleted_modules | set(self._ending_tasks)
        for module in sorted(modules, key=lambda m: m.order, reverse=True):
            try:
                await self._end_module(module)
            except BaseException as exc:
                self._record_error(exc)

    async def shortcut_manager(self) -> None:
        while not self._stopped and self._run_list:
            await self.shortcut_evt.wait()
            if self._stopped or not self._run_list:
                break
            await aio.sleep(SHORTCUT_TIME)
            self._module_selection = None
            self.shortcut_evt.clear()

    @staticmethod
    def _neighbors(module: Module) -> Set[str]:
        "Names of the modules directly connected to ``module`` by a slot"
        names = {slot.output_module.name for slot in module.input_slot_values()}
        for slots in module.output_slot_values():
            for slot in slots or []:
                if slot.input_module is not None:
                    names.add(slot.input_module.name)
        return names

    async def _run_loop(self) -> None:
        """Main scheduler loop."""
        from .module import Module
        # pylint: disable=broad-except
        blocked = 0  # all_blocked() cannot detect that all modules are blocked
        profiler = self.profiler
        # Parallel mode (workers > 1): a module starts on a worker thread as
        # soon as a worker is free and no module directly connected to it is
        # running. Otherwise it is deferred to later in the same sweep, as are
        # the modules whose producer is deferred: each module still runs after
        # its producers, as in the serial order. Callbacks (start_run,
        # after_run, tick procs) and slot preparation stay in this loop.
        running: Dict[aio.Future[None], Tuple[Module, int]] = {}
        deferred: List[Module] = []
        last_order = -1

        def busy(module: Module) -> bool:
            for other, _ in running.values():
                if other is module or other.name in self._neighbors(module):
                    return True
            return any(
                slot.output_module in deferred for slot in module.input_slot_values()
            )

        async def complete(all_running: bool = False) -> None:
            "Wait for at least one running module and finish it"
            # Tick callbacks have unrestricted access to the graph. They may
            # run only once every active worker has left its step.
            wait_all = all_running or bool(self._tick_procs)
            done, _ = await aio.wait(
                list(running),
                return_when=aio.ALL_COMPLETED if wait_all else aio.FIRST_COMPLETED,
            )
            errors: List[BaseException] = []
            successes = []
            for fut in sorted(done, key=lambda f: running[f][1]):
                module, run_number = running.pop(fut)
                if self._active_steps.get(module.name) is fut:
                    del self._active_steps[module.name]
                exc = fut.exception()
                if exc is not None:
                    errors.append(exc)
                else:
                    successes.append((module, run_number))
            if errors:
                raise errors[0]
            for module, run_number in successes:
                await module.after_run(run_number)
            await self._run_tick_procs()

        async def offer(module: Module) -> bool:
            "Start the module if possible; False if it must wait"
            nonlocal blocked
            if self._stopped:
                return True  # paused/aborted: discard unstarted work
            if busy(module):
                return False
            # Unrestricted lifecycle callbacks (including subclass hooks) run
            # with no other module active. Callback-free modules retain task
            # parallelism. Register callbacks before starting the scheduler.
            exclusive = bool(module._start_run or module._after_run) or any(
                getattr(type(module), name, getattr(Module, name))
                is not getattr(Module, name)
                for name in ("start_run", "after_run")
            )
            if exclusive and running:
                await complete(all_running=True)
            if self._stopped:
                return True
            if not self._consider_module(module):
                logger.info(
                    "Module %s not scheduled because of interactive mode",
                    module.name,
                )
            elif self._schedule(module):
                blocked = 0
                await module.start_run(self._run_number)
                if self._stopped:
                    return True
                # Interactive input state belongs to this loop: compute the
                # quantum here rather than in the worker.
                quantum = self.fix_quantum(module, module.params.quantum)
                loop = aio.get_running_loop()
                fut = loop.run_in_executor(
                    self._executor, module.run, self._run_number, quantum
                )
                running[fut] = (module, self._run_number)
                self._active_steps[module.name] = fut
                self._worker_futures.add(fut)
                fut.add_done_callback(self._worker_done)
                if exclusive or len(running) >= self._workers:
                    await complete()
            else:
                blocked += 1
            return True

        async def drain() -> None:
            "Finish the sweep: run the deferred modules, wait for all"
            nonlocal deferred
            while deferred or running:
                pending, deferred = deferred, []
                for module in pending:
                    if not await offer(module):
                        deferred.append(module)
                if running:
                    await complete()

        try:
            async for module in self._next_module(before_update=drain):
                await aio.sleep(0)
                if self._stopped:
                    break
                if module.order <= last_order:  # a new sweep started early
                    await drain()
                last_order = module.order
                if (
                    self.no_more_data()
                    and (self.all_blocked() or blocked == len(self._run_list))
                    and self.is_waiting_for_input()
                ):
                    if self._keep_running <= 0:
                        await drain()
                        async with self._hibernate_cond:
                            await self._hibernate_cond.wait()
                if self._keep_running > 0:
                    self._keep_running -= 1
                if self._workers > 1:
                    if not await offer(module):
                        deferred.append(module)
                elif not self._consider_module(module):
                    logger.info(
                        "Module %s not scheduled because of interactive mode",
                        module.name,
                    )
                elif self._schedule(module):
                    blocked = 0
                    profiler.enable()
                    await module.start_run(self._run_number)
                    if self._stopped:
                        break
                    module.run(self._run_number)
                    await module.after_run(self._run_number)
                    await self._run_tick_procs()
                    profiler.disable()
                else:
                    blocked += 1
                if self._run_index >= len(self._run_list):  # end of the sweep
                    await drain()
                    last_order = -1
            await drain()
            if self.shortcut_evt is not None:
                self.shortcut_evt.set()
        except Exception as exc:
            logger.info("Exception in run loop")
            logger.error(exc)
            raise
        print("Leaving run loop")

    def _schedule(self, module: Module) -> bool:
        "Prepare the module for a run; return True if it should run"
        # increment the run number, even if we don't call the module
        self._run_number += 1
        module.prepare_run(self._run_number)
        if not (module.is_ready() or self.has_input() or module.is_greedy()):
            logger.info(
                "Module %s not scheduled because not ready and has no input",
                module.name,
            )
            return False
        return True

    async def _next_module(
        self, before_update: Optional[Callable[[], Awaitable[None]]] = None
    ) -> AsyncGenerator[Module, None]:
        """
        Generator the yields a possibly infinite sequence of modules.
        Handles order recomputation and starting logic if needed.
        """
        self._run_index = 0
        input_mode = self.has_input()
        self._start_inter = 0
        while not self._stopped:
            # Apply changes in the dataflow
            if self.dataflow is not None and self._enter_cnt == 0:
                # Reconnection and teardown must not race an old graph's
                # workers. Drain before mutating, not after yielding its new
                # first module to the run loop.
                if before_update is not None:
                    await before_update()
                self._update_modules()
                self._run_index = 0
            if self._deleted_modules:
                for mod in self._deleted_modules:
                    await self._end_module(mod)
            if self._added_modules or self._deleted_modules:
                added = self._added_modules
                deleted = self._deleted_modules
                self._added_modules = set()
                self._deleted_modules = set()
                for proc in self._change_procs:
                    await proc(self, added, deleted)
            # If run_list empty, we're done
            if not self._run_list:
                break
            # Check for interactive input mode
            if input_mode != self.has_input():
                if input_mode:  # end input mode
                    logger.info(
                        "Ending interactive mode after %s s",
                        default_timer() - self._start_inter,
                    )
                    self._start_inter = 0
                    input_mode = False
                else:
                    self._start_inter = default_timer()
                    logger.info("Starting interactive mode at %s", self._start_inter)
                    input_mode = True
                # Restart from beginning
                self._run_index = 0
            module = self._run_list[self._run_index]
            self._run_index += 1  # allow it to be reset
            yield module
            if self._run_index >= len(self._run_list):  # end of modules
                await self._end_of_modules()

    def all_blocked(self) -> bool:
        "Return True if all the modules are blocked, False otherwise"
        from .module import Module

        for module in self._run_list:
            if module.state not in (Module.state_blocked, Module.state_suspended):
                return False
        return True

    def is_waiting_for_input(self) -> bool:
        "Return True if there is at least one input module"
        for module in self._run_list:
            if module.is_input():
                # print(f"is_waiting_for_input: {module.name}")
                return True
        # print("is_waiting_for_input: False")
        return False

    def no_more_data(self) -> bool:
        "Return True if at least one module has data input."
        for module in self._run_list:
            if module.is_data_input():
                # print("no_more_data: False")
                return False
        # print("no_more_data: True")
        return True

    def commit(self) -> None:
        """Forces a pending dataflow to be commited

        :returns: None

        """
        if self.dataflow is None or self._enter_cnt > 1:
            return
        self._enter_cnt = 0
        if not self._running:  # no need to delay updating the scheduler
            self._update_modules()

    def _update_modules(self) -> None:
        if self.dataflow is None or self._enter_cnt != 0:
            return
        dataflow = self.dataflow
        _new_runorder = dataflow.order_modules()  # raises if invalid
        _new_modules = list(dataflow.modules().values())
        _new_inputs = dataflow.inputs
        _new_outputs = dataflow.outputs
        dataflow._compute_reachability(_new_inputs)
        _new_reachability = dataflow.reachability
        dataflow.committed()
        self._multiple_slots_name_generator = (
            self.dataflow.multiple_slots_name_generator
        )
        self.dataflow = None
        logger.info("Updating modules")
        prev_keys = set(self._modules.keys())
        modules = {module.name: module for module in _new_modules}
        keys = set(modules.keys())
        added = keys - prev_keys
        deleted = prev_keys - keys
        if deleted:
            logger.info(f"Scheduler deleted modules: {deleted}")
            print(f"# Scheduler deleted module(s): {deleted}")
            self._deleted_modules.update({self[mid] for mid in deleted})
        self._modules = modules
        if not (deleted or added):
            logger.info("Scheduler updated with no new module(s)")
        self._dependencies = _new_inputs
        self._reachability = _new_reachability
        logger.info("New dependencies: %s", self._dependencies)
        for mid, slots in self._dependencies.items():
            modules[mid].reconnect(slots, _new_outputs.get(mid, {}))
        _new_outputs = {}
        _new_inputs = {}
        _new_reachability = {}
        if added:
            logger.info("Scheduler adding modules %s", added)
            sorted_added = sorted(added)
            print(f"# Scheduler added module(s): {sorted_added}")
            for mid in added:
                modules[mid].starting()
            self._added_modules.update({self[mid] for mid in added})
        self._run_list = []
        self._runorder = _new_runorder
        logger.info("New module order: %s", self._runorder)
        for i, mid in enumerate(self._runorder):
            module = self._modules[mid]
            self._run_list.append(module)
            module.order = i
        if not self._run_list:
            print("# Scheduler empty, finishing")

    async def _end_of_modules(self) -> None:
        # Reset interaction mode
        self._selection_target_time = -1
        new_list = [m for m in self._run_list if not m.is_terminated()]
        terminated_list = [m for m in self._run_list if m.is_terminated()]
        for mod in terminated_list:
            await self._end_module(mod)

        self._run_list = new_list
        await self._loop_procs.fire(self, self._run_number)
        if self.all_blocked():
            # no module ready
            has_run = await self._idle_procs.fire(self, self._run_number)
            if (
                not has_run
                and self.no_more_data()
                and not self.is_waiting_for_input()
            ):
                # Nothing outside the dataflow can unblock it: the next sweep
                # terminates the blocked modules, no need to wait.
                pass
            elif not has_run:
                logger.info("sleeping %f", 0.2)
                # print("Sleeping 0.2")
                await aio.sleep(0.2)
        self._run_index = 0

    async def idle_proc_runner(self) -> None:
        has_run = False
        for proc in self._idle_procs:
            # pylint: disable=broad-except
            try:
                logger.debug("Running idle proc")
                res = proc(self, self._run_number)
                if ins.iscoroutine(res):
                    await res
                has_run = True
            except Exception as exc:
                logger.error(exc)
        if not has_run:
            logger.info("sleeping %f", 0.2)
            await aio.sleep(0.2)

    async def _run_tick_procs(self) -> None:
        # pylint: disable=broad-except
        await self._tick_procs.fire(self, self._run_number)

    async def stop(self) -> None:
        """Request a resumable pause; await start's task to join active steps.

        Unstarted work is deferred until the next start(). Cancelling the start
        task instead aborts the dataflow and runs module teardown.
        """
        self._stopped = True
        if self.shortcut_evt is not None:
            self.shortcut_evt.set()
        async with self._hibernate_cond:
            self._keep_running = KEEP_RUNNING
            self._hibernate_cond.notify()
            self._stopped = True

    def task_stop(self) -> Optional[aio.Task[Any]]:
        if self.is_running():
            return aio.create_task(self.stop())
        return None

    def is_running(self) -> bool:
        "Return True if the scheduler is currently running."
        return self._running

    def is_stopped(self) -> bool:
        "Return True if the scheduler is stopped."
        return self._stopped

    def is_terminated(self) -> bool:
        "Return True if the scheduler is terminated."
        for module in self.modules().values():
            if not module.is_terminated():
                return False
        return True

    def done(self) -> None:
        self._task = False
        logger.info("Task finished")

    def __len__(self) -> int:
        return len(self.modules())

    def exists(self, moduleid: str) -> bool:
        "Return True if the moduleid exists in this scheduler."
        return moduleid in self

    def modules(self) -> Dict[str, Module]:
        "Return the dictionary of modules."
        return self._modules

    def __getitem__(self, mid: str) -> Module:
        if self.dataflow is not None:
            return self.dataflow[mid]
        return self._modules[mid]

    def __delitem__(self, name: str) -> None:
        if self.dataflow is not None:
            self.dataflow.delete_modules(name)
        else:
            raise ProgressiveError("Cannot delete module %soutside a context" % name)

    def __contains__(self, name: str) -> bool:
        if self.dataflow is not None:
            return name in self.dataflow
        return name in self._modules

    def groups(self) -> Set[str]:
        return {mod.group for mod in self.modules().values() if mod.group is not None}

    def group_modules(self, *names: str) -> List[str]:
        nameset = set(names)
        if not nameset:
            return []
        return [mod.name for mod in self.modules().values() if mod.group in nameset]

    def run_number(self) -> int:
        """
        Return the last run number.

        Each time a module is run by the scheduler, the `run_number` is incremented.

        """
        return self._run_number

    def _ipython_key_completions_(self) -> List[str]:
        return list(self._modules.keys())

    async def wake_up(self) -> None:
        async with self._hibernate_cond:
            self._keep_running = KEEP_RUNNING
            self._hibernate_cond.notify()

    async def for_input(self, module: Module) -> int:
        """
        Notify this scheduler that the module has received input
        that should be served fast.

        In parallel mode, return only once the module is not running a step
        on a worker: the caller then changes the module before its next step,
        never during one (no await may follow this call before the change).
        """
        await self.wake_up()
        while (step := self._active_steps.get(module.name)) is not None and not step.done():
            await aio.wait([step])
        sel = self._reachability.get(module.name, None)
        if sel:
            if not self._module_selection:
                logger.info("Starting input management")
                self._module_selection = set(sel)
                self._selection_target_time = self.timer() + self.interaction_latency
            else:
                self._module_selection.update(sel)
            logger.debug("Input selection for module: %s", self._module_selection)
        self.shortcut_evt.set()
        return self.run_number() + 1

    def has_input(self) -> bool:
        "Return True of the scheduler is in input mode"
        if self._module_selection is None:
            return False
        if not self._module_selection:  # empty, cleanup
            logger.info("Finishing input management")
            self._module_selection = None
            self._selection_target_time = -1
            return False
        return True

    def _consider_module(self, module: Module) -> bool:
        # FIxME For now, accept all modules in input management
        if not self.has_input():
            return True
        if self._module_selection and module.name in self._module_selection:
            # self._module_selection.remove(module.name)
            logger.debug("Module %s ready for scheduling", module.name)
            return True
        logger.debug("Module %s NOT ready for scheduling", module.name)
        return False

    def time_left(self) -> float:
        "Return the time left to run for this slot."
        if self._selection_target_time <= 0 and not self.has_input():
            logger.error("time_left called with no target time")
            return 0
        return max(0, self._selection_target_time - self.timer())

    def fix_quantum(self, module: Module, quantum: float) -> float:
        "Fix the quantum of the specified module"
        selection = self._module_selection if self.has_input() else None
        if selection and module.name in selection:
            quantum = self.time_left() / len(selection)
        if quantum == 0:
            quantum = 0.1
            logger.info(
                "Quantum is 0 in %s, setting it to a reasonable value", module.name
            )
        return quantum

    def close_all(self) -> None:
        "Close all the resources associated with this scheduler."
        for mod in self.modules().values():
            mod.close_all()

    @staticmethod
    def _module_order(x: Dict[str, int], y: Dict[str, int]) -> int:
        if "order" in x:
            if "order" in y:
                return x["order"] - y["order"]
            return 1
        if "order" in y:
            return -1
        return 0

    @staticmethod
    def module_to_gv(name: str, m: Module, sio: StringIO) -> List[Any]:
        slot_links = []
        sio.write(name)
        sio.write('[shape=Mrecord,label="{{')
        first = True
        inps = set()
        for sn, sl in m._input_slots.items():
            if sl is None:
                continue
            sl_name = sl.input_name
            assert sl_name
            if "." in sn:
                sn = sn.split(".")[0]  # multiple input slot
                sl_name = sl_name.split(".")[0]
                if sn in inps:
                    continue
            inps.add(sn)
            if not first:
                sio.write("|")
            else:
                first = False
            sio.write(f"<i_{sl_name}> {sl_name}")
        sio.write("}|")
        sio.write(f"{name}: {m.__class__.__name__}")
        sio.write("|{")
        first = True
        for sn, slist in m._output_slots.items():
            if sn == "_trace":
                continue
            if not first:
                sio.write("|")
            else:
                first = False
            out_sname = f"{name}:o_{sn}"
            sio.write(f"<o_{sn}> {sn}")
            for sl in slist or []:
                target = sl.input_name
                assert target is not None
                assert sl.input_module is not None
                tname = sl.input_module.name
                if "." in target:
                    target = target.split(".")[0]  # multiple input slot
                slot_links.append((out_sname, f"{tname}:i_{target}"))
        sio.write('}}"];\n')
        return slot_links

    def to_graphviz(self) -> str:
        self._enter_cnt = 0
        self._update_modules()

        sio = StringIO(
            "digraph progressivis {\n"
            "ranksep=1;"
            "node [shape=none"
            ',style="filled"'
            ',fillcolor="#ffffde"'
            ',color="#aaaa33"'
            ",fontname=Helvetica"
            ",fontsize=10"
            "];\n"
        )
        sio.seek(0, SEEK_END)
        slot_links = []
        for name, m in self.modules().items():
            slot_links += self.module_to_gv(name, m, sio)
        for ln1, ln2 in slot_links:
            sio.write(f"{ln1}:s->{ln2}:n;\n")
        sio.write("}\n")
        return sio.getvalue()

    def to_mermaid(self) -> str:
        self._enter_cnt = 0
        self._update_modules()
        lines = []
        slot_links = []
        def_input_slots = {}
        def_output_slots = {}
        for name, m in self.modules().items():
            lines.append(f"subgraph {name} [{m.__class__.__name__}]\n")
            inp_lines = []
            for sn, sl in m._input_slots.items():
                if sl is None:
                    continue
                sl_name = sl.name()
                if "." in sn:
                    sn = sn.split(".")[0]  # multiple input slot
                    sl_name = sl_name.split(".")[0]
                inp_lines.append((sn, sl_name))
                def_input_slots[sl_name] = sn
            if inp_lines:
                lines.append(f"subgraph {name}_inputs [Inputs]\n")
                for _, sl_name in inp_lines:
                    lines.append(f"{sl_name}\n")
                lines.append("end\n")  # end Inputs
            out_lines = []
            for sn, slist in m._output_slots.items():
                if sn == "_trace":
                    continue
                out_sname = f"{name}_out_{sn}"
                out_lines.append((sn, f"{out_sname}\n"))
                def_output_slots[out_sname] = sn
                if not slist:
                    continue
                for sl in slist:
                    target = sl.name()
                    if "." in target:
                        target = target.split(".")[0]  # multiple input slot
                    slot_links.append((out_sname, target))
            if out_lines:
                lines.append(f"subgraph {name}_outputs [Outputs]\n")
                for _, sl_name in out_lines:
                    lines.append(sl_name)
                lines.append("end\n")  # end Outputs
            if inp_lines and out_lines:
                lines.append(f"{name}_inputs -.- {name}_outputs\n")
            lines.append("end\n")  # end module subgraph
        sio = StringIO(
            "flowchart TD\nclassDef outslot fill:#f96,stroke:#333,stroke-width:1px;\n"
        )
        sio.seek(0, SEEK_END)
        for id_name, name in def_input_slots.items():
            sio.write(f"{id_name}({name})\n")
        for id_name, name in def_output_slots.items():
            sio.write(f"{id_name}({name})\n")
        for ln1, ln2 in slot_links:
            sio.write(f"{ln1}-->{ln2}\n")
        for line in lines:
            sio.write(line)
        outslots = ",".join(def_output_slots.keys())
        sio.write(f"class {outslots} outslot\n")
        return sio.getvalue()

    @staticmethod
    def reset() -> None:
        Scheduler.default = Scheduler()


Scheduler.reset()
