"""Compare FeatherStore read/write performance against common storage formats.

Runs write and read benchmarks against CSV, Feather, Parquet, Pickle, DuckDB,
and FeatherStore using pandas as the common interface. Intended for ad-hoc
comparisons; invoke via ``python format_comparison.py`` or ``task bench:format-comparison``.
"""

from common import run_benchmark, table_header
from format_comparison_benches import csv, duckdb, feather, fstore, parquet, pickle


def benchmark_writes(shape, num_partitions=0, plot=False, **kwargs):
    benched = (
        fstore.FeatherStoreWritePandas(shape, num_partitions=num_partitions),
        csv.PandasWriteCsv(shape),
        feather.PandasWriteFeather(shape),
        parquet.PandasWriteParquet(shape),
        pickle.PandasWritePickle(shape),
        duckdb.DuckdbWritePandas(shape),
    )
    return run_benchmark(
        benched,
        table_header("Write benchmark", shape),
        plot=plot,
        **kwargs,
    )


def benchmark_reads(shape, num_partitions=0, plot=False, **kwargs):
    benched = (
        fstore.FeatherStoreReadPandas(shape, num_partitions=num_partitions),
        csv.PandasReadCsv(shape),
        feather.PandasReadFeather(shape),
        parquet.PandasReadParquet(shape),
        pickle.PandasReadPickle(shape),
        duckdb.DuckdbReadPandas(shape),
    )
    return run_benchmark(
        benched,
        table_header("Read benchmark", shape),
        plot=plot,
        **kwargs,
    )


if __name__ == "__main__":
    shape = (100_000, 6)
    num_partitions = 0
    plot = True
    run_kwargs = {
        "n": 3,
        "r": 5,
        "sort": True,
    }
    benchmark_writes(shape, num_partitions, plot=plot, **run_kwargs)
    benchmark_reads(shape, num_partitions, plot=plot, **run_kwargs)
