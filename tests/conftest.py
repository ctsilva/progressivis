"""
Make hangs visible.

ProgressiVis dataflows must always make progress or terminate. A per-test
timeout (pytest-timeout, configured in pyproject.toml) turns a hang into a
failure; this hook then appends the state of the test's scheduler to the
report, so the failure says *why* the dataflow stopped progressing, not only
where Python was waiting.
"""

from __future__ import annotations

from typing import Any, Generator

import pytest


def _describe_scheduler(scheduler: Any) -> str:
    lines = []
    try:
        lines.append(
            f"run_number={scheduler.run_number()} "
            f"is_running={scheduler.is_running()} "
            f"is_terminated={scheduler.is_terminated()}"
        )
        # The conditions under which Scheduler._run_loop hibernates until
        # some input wakes it up (see scheduler.py, _run_loop).
        lines.append(
            f"no_more_data={scheduler.no_more_data()} "
            f"all_blocked={scheduler.all_blocked()} "
            f"is_waiting_for_input={scheduler.is_waiting_for_input()}"
        )
    except Exception as exc:  # report what we can, never mask the timeout
        lines.append(f"<cannot read scheduler status: {exc!r}>")
    for module in scheduler._run_list:
        try:
            lines.append(
                f"  {module.name} [{type(module).__name__}] "
                f"state={module.state.name} last_update={module.last_update()}"
            )
            for slot in module.input_slot_values():
                if slot is None:
                    continue
                src = slot.output_module
                lines.append(
                    f"      <- {slot.input_name} from {src.name}.{slot.output_name}: "
                    f"buffered={slot.has_buffered()} "
                    f"slot.last_update={slot.last_update()} "
                    f"source.last_update={src.last_update()} "
                    f"source.state={src.state.name}"
                )
        except Exception as exc:
            lines.append(f"  {getattr(module, 'name', module)}: <error {exc!r}>")
    return "\n".join(lines)


def pytest_collection_finish(session: pytest.Session) -> None:
    # ProgressiveTest.tearDown runs gc.collect() after every test. Freezing the
    # objects that exist once everything is imported (~200k: numpy, pandas,
    # sklearn, progressivis) makes those collections scan only what each test
    # created: ~25-50 ms -> ~1 ms per test.
    import gc

    gc.collect()
    gc.freeze()


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    # Hand the frozen objects back before interpreter shutdown: finalizing them
    # from the permanent generation crashes some C extensions at exit.
    import gc

    gc.unfreeze()


SLOW_TEST_TIMEOUT = 120  # seconds; the default (pyproject.toml) is for quick tests


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    for item in items:
        if item.get_closest_marker("slow") and not item.get_closest_marker("timeout"):
            item.add_marker(pytest.mark.timeout(SLOW_TEST_TIMEOUT))


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(
    item: pytest.Item, call: pytest.CallInfo[None]
) -> Generator[None, Any, None]:
    outcome = yield
    report = outcome.get_result()
    if not (report.failed and call.excinfo is not None):
        return
    if "Timeout" not in call.excinfo.typename and "Timeout" not in str(
        call.excinfo.value
    ):
        return
    scheduler = getattr(getattr(item, "instance", None), "_scheduler", None)
    if scheduler is None:
        report.sections.append(
            ("progressivis scheduler at timeout", "<no scheduler on test instance>")
        )
        return
    report.sections.append(
        ("progressivis scheduler at timeout", _describe_scheduler(scheduler))
    )
