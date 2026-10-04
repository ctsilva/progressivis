from __future__ import annotations

from os import getenv, environ
import gc
import sys
from unittest import TestCase, main
from unittest import skip as skip
from unittest import skipIf as skipIf
import logging

# The generated "bigfile" dataset has 1M rows by default; 100k rows exercise
# the same code paths much faster. Set PROGRESSIVIS_BIGFILE_ROWS to override.
environ.setdefault("PROGRESSIVIS_BIGFILE_ROWS", "100000")

from progressivis import Scheduler, log_level
from progressivis.storage import init_temp_dir_if, cleanup_temp_dir
import numpy as np

from typing import Any, Type, Optional


def free_port() -> int:
    "A TCP port that is free right now on localhost"
    import socket

    with socket.socket() as sock:
        sock.bind(("localhost", 0))
        return int(sock.getsockname()[1])


class LocalHTTPServer:
    """
    Serve the datasets directory over HTTP, with Range support, from a thread
    of the test process. Bound and ready when start() returns; the port stays
    the same across restart(), so URLs remain valid.
    """

    def __init__(self, port: int, handler: Optional[Type[Any]] = None) -> None:
        self.port = port
        self._handler = handler
        self._server: Any = None
        self._thread: Any = None

    def start(self) -> None:
        import functools
        import http.server
        import threading
        from RangeHTTPServer import RangeRequestHandler  # type: ignore
        from progressivis.datasets import DATA_DIR

        class _Server(http.server.ThreadingHTTPServer):
            allow_reuse_address = True
            daemon_threads = True

        handler: Any = self._handler or RangeRequestHandler
        self._server = _Server(
            ("localhost", self.port), functools.partial(handler, directory=DATA_DIR)
        )
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._thread.join()
            self._server = None

    def restart(self) -> None:
        self.stop()
        self.start()


def taxi_sample(dataset: str) -> str:
    """
    A slice of the first PROGRESSIVIS_TAXI_ROWS rows (default 50k) of a NYC
    taxi Parquet dataset, written once next to it. The tests compute their
    expected values from the file, so its size only changes the run time.
    """
    import os
    import pyarrow.parquet as pq
    from progressivis.datasets import get_dataset

    rows = int(environ.get("PROGRESSIVIS_TAXI_ROWS", "50000"))
    source = get_dataset(dataset)
    stem, ext = os.path.splitext(source)
    target = f"{stem}_{rows}{ext}"
    if not os.path.exists(target):
        table = pq.read_table(source).slice(0, rows)  # type: ignore
        pq.write_table(table, target + ".tmp")  # type: ignore
        os.replace(target + ".tmp", target)
    return target


def taxi_csv_sample() -> str:
    "The taxi_sample() rows as a bz2-compressed CSV file with a header line"
    import os
    import pandas as pd

    source = taxi_sample("short-taxis2015-01_parquet")
    target = os.path.splitext(source)[0] + ".csv.bz2"
    if not os.path.exists(target):
        pd.read_parquet(source).to_csv(target + ".tmp", index=False, compression="bz2")
        os.replace(target + ".tmp", target)
    return target

_ = skip  # shut-up pylint
__ = skipIf


class ProgressiveTest(TestCase):
    CRITICAL = logging.CRITICAL
    ERROR = logging.ERROR
    WARNING = logging.WARNING
    INFO = logging.INFO
    DEBUG = logging.DEBUG
    NOTSET = logging.NOTSET
    levels = {
        "CRITICAL": logging.CRITICAL,
        "ERROR": logging.ERROR,
        "WARNING": logging.WARNING,
        "INFO": logging.INFO,
        "DEBUG": logging.DEBUG,
        "NOTSET": logging.NOTSET,
    }

    def __init__(self, *args: Any) -> None:
        super(ProgressiveTest, self).__init__(*args)
        self._output: bool = False
        self._scheduler: Optional[Scheduler] = None
        self._temp_dir_flag: bool = False
        level: Any = getenv("LOGLEVEL")
        if level in ProgressiveTest.levels:
            level = ProgressiveTest.levels[level]
        else:
            level = None
        if level:
            print(f"Logger level {level} for {self}", file=sys.stderr)
            self.log(int(level))

    @staticmethod
    async def _stop(scheduler: Scheduler, run_number: int) -> None:
        await scheduler.stop()

    def setUp(self) -> None:
        np.random.seed(42)

    def tearDown(self) -> None:
        # print('Logger level for %s back to ERROR' % self, file=sys.stderr)
        # self.log()
        gc.collect()
        logger = logging.getLogger()
        logger.setLevel(logging.NOTSET)
        while logger.hasHandlers():
            logger.removeHandler(logger.handlers[0])

    @classmethod
    def cleanup(self) -> None:
        cleanup_temp_dir()

    @classmethod
    def setUpClass(cls: Type[ProgressiveTest]) -> None:
        cleanup_temp_dir()
        init_temp_dir_if()

    @classmethod
    def tearDownClass(cls: Type[ProgressiveTest]) -> None:
        cleanup_temp_dir()

    @property
    def scheduler(self) -> Scheduler:
        if self._scheduler is None:
            self._scheduler = Scheduler()
        return self._scheduler

    @property
    def clean_scheduler(self) -> Scheduler:
        self._scheduler = Scheduler()
        return self._scheduler

    @staticmethod
    def log(level: int = logging.NOTSET, package: str = "progressivis") -> None:
        log_level(level, package=package)

    @staticmethod
    def main() -> None:
        main()
