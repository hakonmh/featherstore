"""A module used internally to log benchmark results for monitoring performance over time
"""

from internal import *
from featherstore import __version__ as version


def log_benchmark(shape, num_partitions, version, quiet=True):
    run_kwargs = {
        'n': 3,
        'r': 6,
        'sort': False,
        'quiet': quiet,
    }

    bmark = write_bmark(shape, num_partitions=num_partitions, **run_kwargs)
    bmark.log(f'dev/bmarks/Shape{shape} - Partitions({num_partitions})', tag=version)

    bmark = read_bmark(shape, num_partitions=num_partitions, **run_kwargs)
    bmark.log(f'dev/bmarks/Shape{shape} - Partitions({num_partitions})', tag=version)
    bmark = read_rows_bmark(shape, 0.25, num_partitions=num_partitions, **run_kwargs)
    bmark.log(f'dev/bmarks/Shape{shape} - Partitions({num_partitions})', tag=version)
    bmark = read_cols_bmark(shape, 0.25, num_partitions=num_partitions, **run_kwargs)
    bmark.log(f'dev/bmarks/Shape{shape} - Partitions({num_partitions})', tag=version)

    bmark = append_bmark(shape, 0.25, num_partitions=num_partitions, **run_kwargs)
    bmark.log(f'dev/bmarks/Shape{shape} - Partitions({num_partitions})', tag=version)

    bmark = insert_rows_bmark(shape, 0.25, num_partitions=num_partitions, **run_kwargs)
    bmark.log(f'dev/bmarks/Shape{shape} - Partitions({num_partitions})', tag=version)
    bmark = insert_cols_bmark(shape, 0.25, num_partitions=num_partitions, **run_kwargs)
    bmark.log(f'dev/bmarks/Shape{shape} - Partitions({num_partitions})', tag=version)

    bmark = update_rows_bmark(shape, 0.25, num_partitions=num_partitions, **run_kwargs)
    bmark.log(f'dev/bmarks/Shape{shape} - Partitions({num_partitions})', tag=version)
    bmark = update_cols_bmark(shape, 0.25, num_partitions=num_partitions, **run_kwargs)
    bmark.log(f'dev/bmarks/Shape{shape} - Partitions({num_partitions})', tag=version)

    bmark = drop_rows_bmark(shape, 0.25, num_partitions=num_partitions, **run_kwargs)
    bmark.log(f'dev/bmarks/Shape{shape} - Partitions({num_partitions})', tag=version)
    bmark = drop_cols_bmark(shape, 0.25, num_partitions=num_partitions, **run_kwargs)
    bmark.log(f'dev/bmarks/Shape{shape} - Partitions({num_partitions})', tag=version)

    bmark = astype_bmark(shape, 0.25, num_partitions=num_partitions, **run_kwargs)
    bmark.log(f'dev/bmarks/Shape{shape} - Partitions({num_partitions})', tag=version)


if __name__ == '__main__':
    args = [
        ((1_000, 10), 5),
        ((100_000, 50), 500),
        ((1_000_000, 100), 0),
    ]
    for shape, num_partitions in args:
        log_benchmark(shape, num_partitions, version=version, quiet=True)
