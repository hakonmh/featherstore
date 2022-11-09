from external_benches import *
import bmark


def benchmark_writes(shape, num_partitions=0, **kwargs):
    benched = (
        fstore.fs_write_pd(shape, num_partitions=num_partitions),
        csv.pd_write_csv(shape),
        feather.pd_write_feather(shape),
        parquet.pd_write_parquet(shape),
        pickle.pd_write_pickle(shape),
        duckdb.duckdb_write_pd(shape),
        pystore.pystore_write_pd(shape)
    )
    write_bench = bmark.Benchmark(benched)
    header = f'Write benchmark (Table size: {shape[0]:,d}, {shape[1]:,d})'
    write_bench.run(header=header, **kwargs)


def benchmark_reads(shape, num_partitions=0, **kwargs):
    benched = (
        fstore.fs_read_pd(shape, num_partitions=num_partitions),
        csv.pd_read_csv(shape),
        feather.pd_read_feather(shape),
        parquet.pd_read_parquet(shape),
        pickle.pd_read_pickle(shape),
        duckdb.duckdb_read_pd(shape),
        pystore.pystore_read_pd(shape)
    )
    read_bench = bmark.Benchmark(benched)
    header = f'Read benchmark (Table size: {shape[0]:,d}, {shape[1]:,d})'
    read_bench.run(header=header, **kwargs)


if __name__ == '__main__':
    shape = (100_000, 10)
    num_partitions = 5
    run_kwargs = {
        'n': 1,
        'r': 2,
        'sort': True
    }
    benchmark_writes(shape, num_partitions, **run_kwargs)
    benchmark_reads(shape, num_partitions, **run_kwargs)
