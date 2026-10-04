from __future__ import annotations

import os
from . import ProgressiveTest, skipIf, taxi_sample
import pandas as pd
import pyarrow.parquet as pq
from progressivis import Sink, ParquetLoader, PTable
from progressivis.table.group_by import GroupBy, SubPColumn as SC
from progressivis.core import aio
from typing import Any, Tuple


# PARQUET_FILE = "nyc-taxi/short_500k_yellow_tripdata_2015-01.parquet"
PARQUET_FILE = taxi_sample("short-taxis2015-01_parquet")
PARQUET_ROWS = pq.ParquetFile(PARQUET_FILE).metadata.num_rows
PASSENGERS = set(pd.read_parquet(PARQUET_FILE, columns=["passenger_count"]).passenger_count)

# NB: if PARQUET_FILE does not exist yet, consider running:
# python scripts/create_nyc_parquet.py -p short -t yellow -f -m1 -n 300000


@skipIf(os.getenv("CI"), "skipped because local nyc taxi files are required")
class TestProgressiveGroupBy(ProgressiveTest):
    def test_group_by_1_col(self) -> None:
        s = self.scheduler
        parquet = ParquetLoader(
            PARQUET_FILE,
            columns=["passenger_count", "extra", "trip_distance"],
            scheduler=s,
        )
        self.assertTrue(parquet.result is None)
        grby = GroupBy(by="passenger_count", scheduler=s)
        grby.input.table = parquet.output.result
        sink = Sink(scheduler=s)
        sink.input.inp = grby.output.result
        aio.run(s.start())
        assert parquet.result is not None
        self.assertEqual(len(parquet.result), PARQUET_ROWS)
        self.assertEqual(set(grby.index.keys()), PASSENGERS)

    def test_group_by_2_cols(self) -> None:
        s = self.scheduler
        parquet = ParquetLoader(
            PARQUET_FILE,
            columns=["passenger_count", "extra", "trip_distance"],
            scheduler=s,
        )
        self.assertTrue(parquet.result is None)
        grby = GroupBy(by=["passenger_count", "extra"], scheduler=s)
        grby.input.table = parquet.output.result
        sink = Sink(scheduler=s)
        sink.input.inp = grby.output.result
        aio.run(s.start())
        assert parquet.result is not None
        self.assertEqual(len(parquet.result), PARQUET_ROWS)
        expected = pd.read_parquet(PARQUET_FILE, columns=["passenger_count", "extra"])
        n_groups = expected.groupby(["passenger_count", "extra"]).ngroups
        self.assertEqual(len(grby.index.keys()), n_groups)

    def test_group_by_function(self) -> None:
        s = self.scheduler
        parquet = ParquetLoader(
            PARQUET_FILE,
            columns=["passenger_count", "extra", "trip_distance"],
            scheduler=s,
        )
        self.assertTrue(parquet.result is None)
        grby = GroupBy(by=lambda t, i: i % 10, scheduler=s)
        grby.input.table = parquet.output.result
        sink = Sink(scheduler=s)
        sink.input.inp = grby.output.result
        aio.run(s.start())
        assert parquet.result is not None
        self.assertEqual(len(parquet.result), PARQUET_ROWS)
        self.assertEqual(len(grby.index.keys()), 10)

    def test_group_by_days(self) -> None:
        s = self.scheduler
        parquet = ParquetLoader(
            PARQUET_FILE,
            columns=[
                "tpep_pickup_datetime",
                "passenger_count",
                "extra",
                "trip_distance",
            ],
            scheduler=s,
        )

        def _day_func(tbl: PTable, i: int) -> Tuple[Any, ...]:
            dt = tbl.loc[i, "tpep_pickup_datetime"]
            return tuple(dt[:3])

        self.assertTrue(parquet.result is None)
        grby = GroupBy(by=_day_func, scheduler=s)
        grby.input.table = parquet.output.result
        sink = Sink(scheduler=s)
        sink.input.inp = grby.output.result
        aio.run(s.start())
        assert parquet.result is not None
        self.assertEqual(len(parquet.result), PARQUET_ROWS)
        self.assertEqual(
            len(grby.index.keys()), 31
        )

    def test_group_by_dt_ymd(self) -> None:
        s = self.scheduler
        parquet = ParquetLoader(
            PARQUET_FILE,
            columns=[
                "tpep_pickup_datetime",
                "passenger_count",
                "extra",
                "trip_distance",
            ],
            scheduler=s,
        )
        self.assertTrue(parquet.result is None)
        grby = GroupBy(by=SC("tpep_pickup_datetime").dt["YMD"], scheduler=s)
        grby.input.table = parquet.output.result
        sink = Sink(scheduler=s)
        sink.input.inp = grby.output.result
        aio.run(s.start())
        assert parquet.result is not None
        self.assertEqual(len(parquet.result), PARQUET_ROWS)
        self.assertEqual(
            len(grby.index.keys()), 31
        )


if __name__ == "__main__":
    ProgressiveTest.main()
