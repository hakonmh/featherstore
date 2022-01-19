from helpers import *


def write_benchmark(num_rows=1_000_000, num_cols=5, num_runs=1):
    df = generate_df(num_rows, num_cols)

    write_bench = TimeSuiteWrite()
    write_bench.setup()
    runtime = time_it(write_bench.write_table, num_runs, df)

    write_bench.teardown()
    print(f'Write table benchmark (Table size: {num_rows:0,d}, {num_cols:0,d}):\n\n'
          f'  Averaged {runtime:0,.4f} ms per run.\n')


def read_benchmark(num_rows=1_000_000, num_cols=5, num_runs=1):
    df = generate_df(num_rows, num_cols)

    read_bench = TimeSuiteRead()
    read_bench.setup(df)
    pd_runtime = time_it(read_bench.read_pandas, num_runs)
    pa_runtime = time_it(read_bench.read_arrow, num_runs)
    pl_runtime = time_it(read_bench.read_polars, num_runs)
    read_bench.teardown()

    print(f'Read table benchmark (Table size: {num_rows:0,d}, {num_cols:0,d}):\n\n'
          f'  Read Arrow: Averaged {pa_runtime:0,.4f} ms per run.\n'
          f'  Read Pandas: Averaged {pd_runtime:0,.4f} ms per run.\n'
          f'  Read Polars: Averaged {pl_runtime:0,.4f} ms per run.\n')


if __name__ == '__main__':
    shape = (1_000_000, 50)  # Around 4-500 mb of data.
    write_benchmark(num_rows=shape[0], num_cols=shape[1], num_runs=3)
    read_benchmark(num_rows=shape[0], num_cols=shape[1], num_runs=3)
