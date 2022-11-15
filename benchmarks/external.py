from external_benches import *
import bmark


def benchmark_writes(shape, num_partitions=0, plot=False, **kwargs):
    benched = (
        fstore.fs_write_pd(shape, num_partitions=num_partitions),
        csv.pd_write_csv(shape),
        feather.pd_write_feather(shape),
        parquet.pd_write_parquet(shape),
        pickle.pd_write_pickle(shape),
        duckdb.duckdb_write_pd(shape),
    )
    write_bench = bmark.Benchmark(benched)
    header = f'Write benchmark (Table size: {shape[0]:,d}, {shape[1]:,d})'
    quiet = plot
    result = write_bench.run(header=header, quiet=quiet, **kwargs)
    if plot:
        result.plot()
    return result


def benchmark_reads(shape, num_partitions=0, plot=False, **kwargs):
    benched = (
        fstore.fs_read_pd(shape, num_partitions=num_partitions),
        csv.pd_read_csv(shape),
        feather.pd_read_feather(shape),
        parquet.pd_read_parquet(shape),
        pickle.pd_read_pickle(shape),
        duckdb.duckdb_read_pd(shape),
    )
    read_bench = bmark.Benchmark(benched)
    header = f'Read benchmark (Table size: {shape[0]:,d}, {shape[1]:,d})'
    quiet = plot
    result = read_bench.run(header=header, quiet=quiet, **kwargs)
    if plot:
        result.plot()
    return result


if __name__ == '__main__':
    shape = (100_000, 6)
    num_partitions = 0
    plot = True
    run_kwargs = {
        'n': 3,
        'r': 5,
        'sort': True
    }
    benchmark_writes(shape, num_partitions, plot=plot, **run_kwargs)
    benchmark_reads(shape, num_partitions, plot=plot, **run_kwargs)
