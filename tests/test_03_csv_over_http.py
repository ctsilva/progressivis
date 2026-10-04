from . import ProgressiveTest, skipIf, LocalHTTPServer, free_port

import time
import os

from RangeHTTPServer import RangeRequestHandler  # type: ignore


from progressivis.core import aio

from progressivis import CSVLoader, Sink, Constant, PTable
from progressivis.datasets import get_dataset, get_dataset_bz2, bigfile_rows

from typing import Any, Optional


BZ2 = "csv.bz2"
PORT: int = free_port()
HOST: str = "localhost"


class ThrottledReqHandler(RangeRequestHandler):  # type: ignore
    threshold = 10**6
    sleep_times = 3

    def copyfile(self, src: Any, dest: Any) -> None:
        buffer_size = 1024 * 16
        sleep_times = ThrottledReqHandler.sleep_times
        if not self.range:
            cnt = 0
            while True:
                data = src.read(buffer_size)
                if not data:
                    break
                cnt += len(data)
                if sleep_times and cnt > ThrottledReqHandler.threshold:
                    time.sleep(1)
                    sleep_times -= 1
                dest.write(data)
        else:
            RangeRequestHandler.copyfile(self, src, dest)


def _close(module: CSVLoader) -> None:
    try:
        assert module.parser
        module.parser._input._stream.close()
    except Exception:
        pass


def start_server(threshold: Optional[int] = None) -> LocalHTTPServer:
    "Serve the datasets, throttled (pausing after `threshold` bytes) if given"
    _ = get_dataset("smallfile")
    _ = get_dataset("bigfile")
    _ = get_dataset_bz2("smallfile")
    _ = get_dataset_bz2("bigfile")
    if threshold is None:
        server = LocalHTTPServer(PORT)
    else:
        ThrottledReqHandler.threshold = threshold
        server = LocalHTTPServer(PORT, ThrottledReqHandler)
    server.start()
    return server


def make_url(name: str, ext: str = "csv") -> str:
    # The file name depends on the dataset size (see bigfile_rows())
    stem = os.path.splitext(os.path.basename(get_dataset(name)))[0]
    return "http://{host}:{port}/{name}.{ext}".format(
        host=HOST, port=PORT, name=stem, ext=ext
    )


@skipIf(os.getenv("CI"), "cannot run an HTTP local server anymore on CI ...")
class TestProgressiveLoadCSVOverHTTP(ProgressiveTest):
    def setUp(self) -> None:
        super(TestProgressiveLoadCSVOverHTTP, self).setUp()
        self._http_proc: Optional[LocalHTTPServer] = None

    def tearDown(self) -> None:
        if self._http_proc is not None:
            self._http_proc.stop()

    def test_01_read_http_csv_no_crash(self) -> None:
        self._http_proc = start_server()
        s = self.scheduler
        module = CSVLoader(
            make_url("bigfile"), header=None, scheduler=s
        )
        self.assertTrue(module.result is None)
        sink = Sink(name="sink", scheduler=s)
        sink.input.inp = module.output.result
        aio.run(s.start())
        _close(module)
        assert module.result is not None
        self.assertEqual(len(module.result), bigfile_rows())

    def test_02_read_http_csv_crash_recovery(self) -> None:
        self._http_proc = start_server(threshold=10**7)
        s = self.scheduler
        module = CSVLoader(
            make_url("bigfile"), header=None, scheduler=s, timeout=0.01
        )
        self.assertTrue(module.result is None)
        sink = Sink(name="sink", scheduler=s)
        sink.input.inp = module.output.result
        aio.run(s.start())
        _close(module)
        assert module.result is not None
        self.assertEqual(len(module.result), bigfile_rows())

    def test_03_read_multiple_csv_crash_recovery(self) -> None:
        self._http_proc = start_server(threshold=10**6)
        s = self.scheduler
        filenames = PTable(
            name="file_names",
            dshape="{filename: string}",
            data={"filename": [make_url("smallfile"), make_url("smallfile")]},
        )
        cst = Constant(table=filenames, scheduler=s)
        csv = CSVLoader(header=None, scheduler=s, timeout=0.01)
        csv.input.filenames = cst.output.result
        sink = Sink(name="sink", scheduler=s)
        sink.input.inp = csv.output.result
        aio.run(csv.start())
        _close(csv)
        assert csv.result is not None
        self.assertEqual(len(csv.result), 60000)

    def test_04_read_http_csv_bz2_no_crash(self) -> None:
        self._http_proc = start_server()
        s = self.scheduler
        module = CSVLoader(
            make_url("bigfile", ext=BZ2), header=None, scheduler=s
        )
        self.assertTrue(module.result is None)
        sink = Sink(name="sink", scheduler=s)
        sink.input.inp = module.output.result
        aio.run(s.start())
        _close(module)
        assert module.result is not None
        self.assertEqual(len(module.result), bigfile_rows())

    def test_05_read_http_csv_bz2_crash_recovery(self) -> None:
        self._http_proc = start_server(threshold=10**7)
        s = self.scheduler
        module = CSVLoader(
            make_url("bigfile", ext=BZ2),
            header=None,
            scheduler=s,
            timeout=0.01,
        )
        self.assertTrue(module.result is None)
        sink = Sink(name="sink", scheduler=s)
        sink.input.inp = module.output.result
        aio.run(s.start())
        _close(module)
        assert module.result is not None
        self.assertEqual(len(module.result), bigfile_rows())

    def test_06_read_multiple_csv_bz2_crash_recovery(self) -> None:
        self._http_proc = start_server(threshold=10**6)
        s = self.scheduler
        filenames = PTable(
            name="file_names",
            dshape="{filename: string}",
            data={
                "filename": [
                    make_url("smallfile", ext=BZ2),
                    make_url("smallfile", ext=BZ2),
                ]
            },
        )
        cst = Constant(table=filenames, scheduler=s)
        csv = CSVLoader(header=None, scheduler=s, timeout=0.01)
        csv.input.filenames = cst.output.result
        sink = Sink(name="sink", scheduler=s)
        sink.input.inp = csv.output.result
        aio.run(csv.start())
        _close(csv)
        assert csv.result is not None
        self.assertEqual(len(csv.result), 60000)


if __name__ == "__main__":
    ProgressiveTest.main()
