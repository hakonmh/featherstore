from internal_benches import *
import numpy as np
import pyarrow as pa


def read_bmark(shape, ratio=None, num_partitions=0, **kwargs):
    _read_fs()

    read.read_bench.setup(shape, num_partitions=num_partitions)
    header = f'Full read benchmark (Table size: {shape[0]:,d}, {shape[1]:,d})'
    result = read.read_bench.run(header, **kwargs)
    read.read_bench.teardown()
    return result


def read_rows_bmark(shape, ratio, num_partitions=0, **kwargs):
    _do_rows(_read_fs, shape, ratio)

    read.read_bench.setup(shape, num_partitions=num_partitions)
    header = f'Partial read rows benchmark (Table size: {shape[0]:,d}, {shape[1]:,d})'
    result = read.read_bench.run(header, **kwargs)
    read.read_bench.teardown()
    return result


def read_cols_bmark(shape, ratio, num_partitions=0, **kwargs):
    _do_cols(_read_fs, shape, ratio)

    read.read_bench.setup(shape, num_partitions=num_partitions)
    header = f'Partial read columns benchmark (Table size: {shape[0]:,d}, {shape[1]:,d})'
    result = read.read_bench.run(header, **kwargs)
    read.read_bench.teardown()
    return result


def _read_fs(*args, **kwargs):
    read.read_arrow(*args, **kwargs)
    read.read_pandas(*args, **kwargs)
    read.read_polars(*args, **kwargs)


def write_bmark(shape, ratio=None, num_partitions=0, **kwargs):
    write.write_arrow(shape, num_partitions=num_partitions)
    write.write_pandas(shape, num_partitions=num_partitions)
    write.write_polars(shape, num_partitions=num_partitions)
    header = f'Write benchmark (Table size: {shape[0]:,d}, {shape[1]:,d})'
    result = write.write_bench.run(header=header, **kwargs)
    return result


def append_bmark(shape, ratio, num_partitions=0, **kwargs):
    num_rows_to_append = round(shape[0] * ratio)

    append.append_arrow(shape, num_rows_to_append, num_partitions=num_partitions)
    append.append_pandas(shape, num_rows_to_append, num_partitions=num_partitions)
    append.append_polars(shape, num_rows_to_append, num_partitions=num_partitions)

    header = f'Append benchmark (Table size: {shape[0]:,d}, {shape[1]:,d})'
    result = append.append_bench.run(header, **kwargs)
    return result


def insert_rows_bmark(shape, ratio, num_partitions=0, **kwargs):
    _do_rows(insert.insert, shape, ratio, num_partitions=num_partitions)
    header = f'Insert rows benchmark (Table size: {shape[0]:,d}, {shape[1]:,d})'
    result = insert.insert_bench.run(header, **kwargs)
    return result


def insert_cols_bmark(shape, ratio, num_partitions=0, **kwargs):
    _do_cols(insert.insert, shape, ratio, num_partitions=num_partitions)
    header = f'Insert columns benchmark (Table size: {shape[0]:,d}, {shape[1]:,d})'
    result = insert.insert_bench.run(header, **kwargs)
    return result


def update_rows_bmark(shape, ratio, num_partitions=0, **kwargs):
    _do_rows(update.update, shape, ratio, num_partitions=num_partitions)
    header = f'Update rows benchmark (Table size: {shape[0]:,d}, {shape[1]:,d})'
    result = update.update_bench.run(header, **kwargs)
    return result


def update_cols_bmark(shape, ratio, num_partitions=0, **kwargs):
    _do_cols(update.update, shape, ratio, num_partitions=num_partitions)
    header = f'Update columns benchmark (Table size: {shape[0]:,d}, {shape[1]:,d})'
    result = update.update_bench.run(header, **kwargs)
    return result


def drop_rows_bmark(shape, ratio, num_partitions=0, **kwargs):
    _do_rows(drop.drop, shape, ratio, num_partitions=num_partitions)
    header = f'Drop rows benchmark (Table size: {shape[0]:,d}, {shape[1]:,d})'
    result = drop.drop_bench.run(header, **kwargs)
    return result


def drop_cols_bmark(shape, ratio, num_partitions=0, **kwargs):
    _do_cols(drop.drop, shape, ratio, num_partitions=num_partitions)
    header = f'Drop columns benchmark (Table size: {shape[0]:,d}, {shape[1]:,d})'
    result = drop.drop_bench.run(header, **kwargs)
    return result


def astype_bmark(shape, ratio, num_partitions=0, **kwargs):
    cols = np.random.choice(range(shape[1]), round(shape[1] * ratio), replace=False)
    cols_to_change_dtype = [f'c{n}' for n in cols]

    TYPE_MAP = (
        (np.int64, np.int32),
        (pa.uint32(), pa.int64()),
        (pa.float64(), pa.float32()),
        (pa.timestamp('us'), pa.date64()),
        (pa.string(), pa.binary()),
        (pa.large_string(), pa.string()),
    )
    for dtype, to in TYPE_MAP:
        astype.astype(shape, cols_to_change_dtype, dtype=dtype, to=to,
                      num_partitions=num_partitions)

    header = f'Change dtype benchmark (Table size: {shape[0]:,d}, {shape[1]:,d})'
    result = astype.astype_bench.run(header, **kwargs)
    return result


def _do_rows(func, shape, ratio, **kwargs):
    start = round(shape[0] * ratio)
    stop = start * 2
    after = shape[0] - start
    rows_list = list(range(start, stop))

    func(shape=shape, rows={'before': start}, name=f'(before {start:,d})', **kwargs)
    func(shape=shape, rows={'after': after}, name=f'(after {after:,d})', **kwargs)
    func(shape=shape, rows={'between': [start, stop]}, name=f'(between {start:,d}-{stop:,d})', **kwargs)
    func(shape=shape, rows=rows_list, name=f'(list of {len(rows_list):,d} rows)', **kwargs)


def _do_cols(func, shape, ratio, **kwargs):
    cols = [f'c{n}' for n in range(shape[1])]
    num_cols = round(len(cols) * ratio)
    cols = np.random.choice(cols, num_cols, replace=False)
    func(shape=shape, cols=cols, name=f'(list of {num_cols:,d} cols)', **kwargs)


def run_bmarks(shape, num_partitions, ratio=0.25, run='all', **kwargs):
    bmarks = {
        'read': [read_bmark, read_rows_bmark, read_cols_bmark],
        'read[full]': [read_bmark],
        'read[rows]': [read_rows_bmark],
        'read[cols]': [read_cols_bmark],
        'write': [write_bmark],
        'append': [append_bmark],
        'insert': [insert_rows_bmark, insert_cols_bmark],
        'insert[rows]': [insert_rows_bmark],
        'insert[cols]': [insert_cols_bmark],
        'update': [update_rows_bmark, update_cols_bmark],
        'update[rows]': [update_rows_bmark],
        'update[cols]': [update_cols_bmark],
        'drop': [drop_rows_bmark, drop_cols_bmark],
        'drop[rows]': [drop_rows_bmark],
        'drop[cols]': [drop_cols_bmark],
        'astype': [astype_bmark],
    }
    if run == 'all':
        funcs = list({i: None for value in bmarks.values() for i in value})
    else:
        funcs = bmarks[run]
    for func in funcs:
        func(shape=shape, ratio=ratio, num_partitions=num_partitions, **kwargs)


if __name__ == '__main__':
    KWARGS = {
        'n': 3,
        'r': 5,
        'sort': False,
        'quiet': False
    }
    run_bmarks(
        run='all',
        shape=(1_000, 12),
        num_partitions=20,
        **KWARGS
    )
