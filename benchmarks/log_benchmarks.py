"""Run table-operation benchmarks and persist results for performance tracking.

Executes a fixed suite of read, write, and mutation benchmarks at predefined
table shapes and writes timing logs under ``.dev/bmarks``, tagged with the
current FeatherStore version. Invoke via ``python log_benchmarks.py`` or
``task bench:log``.
"""

import os

from featherstore import __version__ as version

from table_operations import (
    append_bmark,
    insert_cols_bmark,
    insert_rows_bmark,
    read_bmark,
    read_cols_bmark,
    read_rows_bmark,
    write_bmark,
)

BENCHMARKS_PATH = ".dev/bmarks"

RUN_KWARGS = {
    "n": 3,
    "r": 6,
    "sort": False,
}

BENCH_FUNCS = [
    (write_bmark, ()),
    (read_bmark, ()),
    (read_rows_bmark, (0.25,)),
    (read_cols_bmark, (0.25,)),
    (append_bmark, (0.25,)),
    (insert_rows_bmark, (0.25,)),
    (insert_cols_bmark, (0.25,)),
]


def log_benchmark(shape, num_partitions, version, quiet=True):
    run_kwargs = {**RUN_KWARGS, "quiet": quiet}
    os.makedirs(BENCHMARKS_PATH, exist_ok=True)
    path = f"{BENCHMARKS_PATH}/Shape{shape} - Partitions({num_partitions})"

    print("Running benchmarks...")
    for func, extra_args in BENCH_FUNCS:
        print(f"Running {func.__name__} benchmark...")
        if extra_args:
            (ratio,) = extra_args
            result = func(shape, ratio, num_partitions=num_partitions, **run_kwargs)
        else:
            result = func(shape, num_partitions=num_partitions, **run_kwargs)

        result.log(path, tag=version)
        print(f"Logged benchmark results to {path}...")


if __name__ == "__main__":
    configs = [
        ((1_000, 10), 5),
        ((100_000, 50), 500),
        ((1_000_000, 100), 0),
    ]
    for shape, num_partitions in configs:
        log_benchmark(shape, num_partitions, version=version, quiet=True)
