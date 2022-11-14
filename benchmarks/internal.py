from internal_benches import *
import numpy as np
import pyarrow as pa


def write_bmark(shape, num_partitions=0, **kwargs):
    write.write_arrow(shape, num_partitions=num_partitions)
    write.write_pandas(shape, num_partitions=num_partitions)
    write.write_polars(shape, num_partitions=num_partitions)
    header = f'Write benchmark (Table size: {shape[0]:,d}, {shape[1]:,d})'
    result = write.write_bench.run(header=header, **kwargs)
    return result


def read_bmark(shape, num_partitions=0, **kwargs):
    _read_fs()

    read.read_bench.setup(shape, num_partitions=num_partitions)
    header = f'Full read benchmark (Table size: {shape[0]:,d}, {shape[1]:,d})'
    result = read.read_bench.run(header, **kwargs)
    read.read_bench.teardown()
    return result


def read_rows_bmark(shape, rows_ratio, num_partitions=0, **kwargs):
    _do_rows(_read_fs, shape, rows_ratio)

    read.read_bench.setup(shape, num_partitions=num_partitions)
    header = f'Partial read rows benchmark (Table size: {shape[0]:,d}, {shape[1]:,d})'
    result = read.read_bench.run(header, **kwargs)
    read.read_bench.teardown()
    return result


def read_cols_bmark(shape, cols_ratio, num_partitions=0, **kwargs):
    _do_cols(_read_fs, shape, cols_ratio)

    read.read_bench.setup(shape, num_partitions=num_partitions)
    header = f'Partial read columns benchmark (Table size: {shape[0]:,d}, {shape[1]:,d})'
    result = read.read_bench.run(header, **kwargs)
    read.read_bench.teardown()
    return result


def _read_fs(*args, **kwargs):
    read.read_arrow(*args, **kwargs)
    read.read_pandas(*args, **kwargs)
    read.read_polars(*args, **kwargs)


def append_bmark(shape, append_ratio, num_partitions=0, **kwargs):
    num_rows_to_append = round(shape[0] * append_ratio)

    append.append_arrow(shape, num_rows_to_append, num_partitions=num_partitions)
    append.append_pandas(shape, num_rows_to_append, num_partitions=num_partitions)
    append.append_polars(shape, num_rows_to_append, num_partitions=num_partitions)

    header = f'Append benchmark (Table size: {shape[0]:,d}, {shape[1]:,d})'
    result = append.append_bench.run(header, **kwargs)
    return result


def insert_rows_bmark(shape, insert_ratio, num_partitions=0, **kwargs):
    _do_rows(insert.insert, shape, insert_ratio, shape=shape, num_partitions=num_partitions)
    header = f'Insert rows benchmark (Table size: {shape[0]:,d}, {shape[1]:,d})'
    result = insert.insert_bench.run(header, **kwargs)
    return result


def insert_cols_bmark(shape, insert_ratio, num_partitions=0, **kwargs):
    _do_cols(insert.insert, shape, insert_ratio, shape=shape, num_partitions=num_partitions)
    header = f'Insert columns benchmark (Table size: {shape[0]:,d}, {shape[1]:,d})'
    result = insert.insert_bench.run(header, **kwargs)
    return result


def update_rows_bmark(shape, update_ratio, num_partitions=0, **kwargs):
    _do_rows(update.update, shape, update_ratio, shape=shape, num_partitions=num_partitions)
    header = f'Update rows benchmark (Table size: {shape[0]:,d}, {shape[1]:,d})'
    result = update.update_bench.run(header, **kwargs)
    return result


def update_cols_bmark(shape, update_ratio, num_partitions=0, **kwargs):
    _do_cols(update.update, shape, update_ratio, shape=shape, num_partitions=num_partitions)
    header = f'Update columns benchmark (Table size: {shape[0]:,d}, {shape[1]:,d})'
    result = update.update_bench.run(header, **kwargs)
    return result


def drop_rows_bmark(shape, drop_ratio, num_partitions=0, **kwargs):
    _do_rows(drop.drop, shape, drop_ratio, shape=shape, num_partitions=num_partitions)
    header = f'Drop rows benchmark (Table size: {shape[0]:,d}, {shape[1]:,d})'
    result = drop.drop_bench.run(header, **kwargs)
    return result


def drop_cols_bmark(shape, drop_ratio, num_partitions=0, **kwargs):
    _do_cols(drop.drop, shape, drop_ratio, shape=shape, num_partitions=num_partitions)
    header = f'Drop columns benchmark (Table size: {shape[0]:,d}, {shape[1]:,d})'
    result = drop.drop_bench.run(header, **kwargs)
    return result


def astype_bmark(shape, astype_ratio, num_partitions=0, **kwargs):
    cols = [f'c{n}' for n in range(shape[1])]
    num_cols_to_update = round(len(cols) * astype_ratio)
    cols_to_update = np.random.choice(cols, num_cols_to_update, replace=False)

    astype.astype(shape, cols_to_update, dtype=np.int64, to=np.int32,
                  num_partitions=num_partitions)
    astype.astype(shape, cols_to_update, dtype=pa.uint32(), to=pa.int64(),
                  num_partitions=num_partitions)
    astype.astype(shape, cols_to_update, dtype=pa.float64(), to=pa.float32(),
                  num_partitions=num_partitions)
    astype.astype(shape, cols_to_update, dtype=pa.timestamp('us'), to=pa.date64(),
                  num_partitions=num_partitions)
    astype.astype(shape, cols_to_update, dtype=pa.string(), to=pa.binary(),
                  num_partitions=num_partitions)
    astype.astype(shape, cols_to_update, dtype=pa.large_string(), to=pa.string(),
                  num_partitions=num_partitions)

    header = f'Change dtype benchmark (Table size: {shape[0]:,d}, {shape[1]:,d})'
    result = astype.astype_bench.run(header, **kwargs)
    return result


def _do_rows(func, shp, ratio, **kwargs):
    start = round(shp[0] * ratio)
    stop = start * 2
    after = shp[0] - start
    rows_list = list(range(start, stop))

    func(rows={'before': start}, name=f'(before {start:,d})', **kwargs)
    func(rows={'after': after}, name=f'(after {after:,d})', **kwargs)
    func(rows={'between': [start, stop]}, name=f'(between {start:,d}-{stop:,d})', **kwargs)
    func(rows=rows_list, name=f'(list of {len(rows_list):,d} rows)', **kwargs)


def _do_cols(func, shp, ratio, **kwargs):
    cols = [f'c{n}' for n in range(shp[1])]
    num_cols = round(len(cols) * ratio)
    cols = np.random.choice(cols, num_cols, replace=False)

    func(cols=cols, name=f'(list of {num_cols:,d} cols)', **kwargs)


if __name__ == '__main__':
    shape = (100_000, 50)
    num_partitions = 5
    run_kwargs = {
        'n': 1,
        'r': 4,
        'sort': False
    }

    write_bmark(shape, num_partitions=num_partitions, **run_kwargs)

    read_bmark(shape, num_partitions=num_partitions, **run_kwargs)
    read_rows_bmark(shape, 0.25, num_partitions=num_partitions, **run_kwargs)
    read_cols_bmark(shape, 0.25, num_partitions=num_partitions, **run_kwargs)

    append_bmark(shape, 0.25, num_partitions=num_partitions, **run_kwargs)

    insert_rows_bmark(shape, 0.25, num_partitions=num_partitions, **run_kwargs)
    insert_cols_bmark(shape, 0.25, num_partitions=num_partitions, **run_kwargs)

    update_rows_bmark(shape, 0.25, num_partitions=num_partitions, **run_kwargs)
    update_cols_bmark(shape, 0.25, num_partitions=num_partitions, **run_kwargs)

    drop_rows_bmark(shape, 0.25, num_partitions=num_partitions, **run_kwargs)
    drop_cols_bmark(shape, 0.25, num_partitions=num_partitions, **run_kwargs)

    astype_bmark(shape, 0.25, num_partitions=num_partitions, **run_kwargs)
