"""
Check the properties that progressive results are expected to have
(design/parallel_progressivis.md, "Guarantees"):

- prefix consistency: after every step, an exact operator's output equals the
  batch result on the rows it has consumed so far;
- eventual exactness: at the end, it equals the batch result on all rows;
- interleaving independence: the final result does not depend on how the run
  was split into steps.

Step sizes are randomized (with fixed seeds) to exercise many interleavings.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Tuple

import numpy as np
from . import ProgressiveTest
from progressivis import Max, Min, RandomPTable, Scheduler, Sink, Var
from progressivis.core import aio
from progressivis.core.module import Module
from progressivis.core.pintset import PIntSet
from progressivis.table.aggregate import Aggregate
from progressivis.table.group_by import GroupBy
from progressivis.table.stirrer import Stirrer
from progressivis.table.table import PTable

ROWS = 20_000
COLS = 3


def _random_steps(module: Module, seed: int, high: int = 3_000) -> None:
    "Make the module consume a random number of rows at each step"
    rng = np.random.default_rng(seed)
    setattr(module, "predict_step_size", lambda quantum: int(rng.integers(1, high)))


def _consumed(module: Module, slot_name: str = "table") -> PIntSet:
    "Rows of the input table that the module has processed so far"
    slot = module.get_input_slot(slot_name)
    table = slot.data()
    pending = slot.created.changes if slot.created.buffer else PIntSet()
    return PIntSet(table.index) - pending


BatchFn = Callable[[np.ndarray[Any, Any]], np.ndarray[Any, Any]]


class TestPrefixConsistency(ProgressiveTest):
    def _check(self, make: Callable[[Scheduler], Module], batch: BatchFn, seed: int) -> None:
        s = self.scheduler
        source = RandomPTable(COLS, rows=ROWS, scheduler=s)
        module: Any = make(s)
        module.input.table = source.output.result
        _random_steps(module, seed)
        sink = Sink(scheduler=s)
        sink.input.inp = module.output.result
        mismatches: List[str] = []
        steps = [0]

        def after_run(mod: Any, run_number: int) -> None:
            consumed = _consumed(mod)
            if not consumed or mod.result is None:
                return
            steps[0] += 1
            table = mod.get_input_slot("table").data()
            data = table.loc[consumed].to_array()
            expected = batch(data)
            got = np.array([mod.result[c] for c in table.columns])
            if not np.allclose(got, expected):
                mismatches.append(
                    f"step {steps[0]}: {len(consumed)} rows, got {got}, expected {expected}"
                )

        module.on_after_run(after_run)
        aio.run(s.start())
        self.assertGreater(steps[0], 3, "too few steps to test progression")
        self.assertEqual(mismatches, [])
        # eventual exactness
        table = source.result
        assert table is not None and module.result is not None
        got = np.array([module.result[c] for c in table.columns])
        self.assertTrue(np.allclose(got, batch(table.to_array())))

    def test_min(self) -> None:
        for seed in (1, 2, 3):
            with self.subTest(seed=seed):
                self._scheduler = None
                self._check(lambda s: Min(scheduler=s), lambda a: a.min(axis=0), seed)

    def test_max(self) -> None:
        for seed in (1, 2, 3):
            with self.subTest(seed=seed):
                self._scheduler = None
                self._check(lambda s: Max(scheduler=s), lambda a: a.max(axis=0), seed)

    def test_var(self) -> None:
        for seed in (1, 2, 3):
            with self.subTest(seed=seed):
                self._scheduler = None
                self._check(
                    lambda s: Var(scheduler=s), lambda a: a.var(axis=0, ddof=0), seed
                )


def _aggregate_with_delete(seed: int, removed: int) -> Tuple[Dict[float, float], Dict[float, float]]:
    """
    RandomPTable -> Stirrer (deletes one row) -> GroupBy -> Aggregate(sum),
    with random step sizes. Returns (progressive result, batch result on the
    table after the deletion). The data is the same for every seed.
    """
    np.random.seed(42)
    s = Scheduler()
    source = RandomPTable(
        2,
        rows=ROWS,
        random=lambda size: np.asarray(np.random.randint(0, 5, size=size), dtype="float64"),
        scheduler=s,
    )
    stirrer = Stirrer(
        update_column="_2", delete_rows=[removed], fixed_step_size=1000, scheduler=s
    )
    stirrer.input[0] = source.output.result
    _random_steps(stirrer, seed + 200)
    grby = GroupBy(by="_1", scheduler=s)
    grby.input.table = stirrer.output.result
    _random_steps(grby, seed)
    aggr = Aggregate(compute=[("_2", "sum")], scheduler=s)
    aggr.input.table = grby.output.result
    _random_steps(aggr, seed + 100)
    sink = Sink(scheduler=s)
    sink.input.inp = aggr.output.result
    aio.run(s.start())
    assert isinstance(aggr.result, PTable) and stirrer.result is not None
    assert removed not in stirrer.result.index, "the requested row was not deleted"
    got = dict(
        zip(aggr.result["_1"].value.tolist(), aggr.result["_2_sum"].value.tolist())
    )
    after = stirrer.result.to_array()
    expected: Dict[float, float] = {}
    for key, value in after:
        expected[key] = expected.get(key, 0.0) + value
    return got, expected


class TestInterleavingIndependence(ProgressiveTest):
    def test_groupby_aggregate_with_delete(self) -> None:
        "Whatever the step sizes, the result equals the batch result after deletion"
        # the last row always arrives in the last step: deleting it is the
        # case that used to be lost (Stirrer deferred it, then never ran again)
        for removed in (1234, ROWS - 1):
            for seed in range(3):
                with self.subTest(removed=removed, seed=seed):
                    got, expected = _aggregate_with_delete(seed, removed=removed)
                    self.assertEqual(sorted(got), sorted(expected))
                    for k in got:
                        self.assertAlmostEqual(got[k], expected[k])


if __name__ == "__main__":
    ProgressiveTest.main()
