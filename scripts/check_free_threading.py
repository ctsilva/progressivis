"""
Check that ProgressiVis can run without the GIL on a free-threaded CPython.

Imports every progressivis module without PYTHON_GIL set and fails if an
extension module re-enables the GIL, except the known ones listed below
(these must then be run with PYTHON_GIL=0, at our own risk).

Usage: python scripts/check_free_threading.py
"""

from __future__ import annotations

import importlib
import os
import pkgutil
import re
import sys
import sysconfig
import warnings

# Extension modules known to re-enable the GIL: not declared free-threading
# safe by their maintainers. The test suite runs with PYTHON_GIL=0 regardless.
KNOWN = {"_datasketches"}

GIL_WARNING = re.compile(r"enabled to load module '([^']+)'")


def main() -> int:
    if not sysconfig.get_config_var("Py_GIL_DISABLED"):
        print("Not a free-threaded build of CPython")
        return 1
    if "PYTHON_GIL" in os.environ or sys.flags.gil == 0:
        print("Run without PYTHON_GIL / -X gil: the check needs the default behavior")
        return 1
    reenabled: set[str] = set()
    skipped: list[str] = []
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", RuntimeWarning)
        import progressivis

        for info in pkgutil.walk_packages(progressivis.__path__, "progressivis."):
            try:
                importlib.import_module(info.name)
            except ImportError as exc:  # optional dependency missing
                skipped.append(f"{info.name}: {exc}")
    for w in caught:
        match = GIL_WARNING.search(str(w.message))
        if match:
            reenabled.add(match.group(1))
    for line in skipped:
        print("skipped", line)
    print("GIL re-enabled by:", sorted(reenabled) or "none")
    unexpected = reenabled - KNOWN
    if unexpected:
        print("FAIL: new modules re-enable the GIL:", sorted(unexpected))
        return 1
    if KNOWN - reenabled:
        print("Note: no longer re-enable the GIL (update KNOWN):", sorted(KNOWN - reenabled))
    return 0


if __name__ == "__main__":
    sys.exit(main())
