"""Benchmark FeatherStore table operations across Arrow, Pandas, and Polars.

Covers read (full, partial rows/columns), write (sorted and unsorted), append,
insert, update, drop, and dtype conversion. Use ``run_bmarks()`` to select a
subset, or invoke via ``python table_operations.py`` or ``task bench:table-operations``.
"""

import numpy as np
import pyarrow as pa
from common import table_header
from table_operations_benches import append, astype, drop, insert, read, update, write

ASTYPE_CONVERSIONS = (
    (np.int64, np.int32),
    (pa.uint32(), pa.int64()),
    (pa.float64(), pa.float32()),
    (pa.timestamp("us"), pa.date64()),
    (pa.string(), pa.binary()),
    (pa.large_string(), pa.string()),
)


def _run_read_benchmark(configure, shape, num_partitions, header_name, **kwargs):
    configure()
    read.read_bench.setup(shape, num_partitions=num_partitions)
    result = read.read_bench.run(table_header(header_name, shape), **kwargs)
    read.read_bench.teardown()
    return result


def _register_read_variants(**kwargs):
    read.ReadArrow(**kwargs)
    read.ReadPandas(**kwargs)
    read.ReadPolars(**kwargs)


def read_bmark(shape, ratio=None, num_partitions=0, **kwargs):
    return _run_read_benchmark(
        _register_read_variants,
        shape,
        num_partitions,
        "Full read benchmark",
        **kwargs,
    )


def read_rows_bmark(shape, ratio, num_partitions=0, **kwargs):
    return _run_read_benchmark(
        lambda: _configure_rows(_register_read_variants, shape, ratio),
        shape,
        num_partitions,
        "Partial read rows benchmark",
        **kwargs,
    )


def read_cols_bmark(shape, ratio, num_partitions=0, **kwargs):
    return _run_read_benchmark(
        lambda: _configure_cols(_register_read_variants, shape, ratio),
        shape,
        num_partitions,
        "Partial read columns benchmark",
        **kwargs,
    )


def write_bmark(shape, ratio=None, num_partitions=0, **kwargs):
    write.WriteArrow(shape, num_partitions=num_partitions)
    write.WritePandas(shape, num_partitions=num_partitions)
    write.WritePolars(shape, num_partitions=num_partitions)
    return write.write_bench.run(
        header=table_header("Write benchmark", shape),
        **kwargs,
    )


def write_unsorted_bmark(shape, ratio=None, num_partitions=0, **kwargs):
    write.WriteArrow(shape, sorted=False, num_partitions=num_partitions)
    write.WritePandas(shape, sorted=False, num_partitions=num_partitions)
    write.WritePolars(shape, sorted=False, num_partitions=num_partitions)
    return write.write_bench.run(
        header=table_header("Write unsorted benchmark", shape),
        **kwargs,
    )


def append_bmark(shape, ratio, num_partitions=0, **kwargs):
    num_rows_to_append = round(shape[0] * ratio)

    append.AppendArrow(shape, num_rows_to_append, num_partitions=num_partitions)
    append.AppendPandas(shape, num_rows_to_append, num_partitions=num_partitions)
    append.AppendPolars(shape, num_rows_to_append, num_partitions=num_partitions)

    return append.append_bench.run(
        table_header("Append benchmark", shape),
        **kwargs,
    )


def _run_mutation_benchmark(
    bench, register, target, shape, ratio, header_name, num_partitions=0, **kwargs
):
    register(target, shape, ratio, num_partitions=num_partitions)
    return bench.run(table_header(header_name, shape), **kwargs)


def insert_rows_bmark(shape, ratio, num_partitions=0, **kwargs):
    return _run_mutation_benchmark(
        insert.insert_bench,
        _configure_rows,
        insert.Insert,
        shape,
        ratio,
        "Insert rows benchmark",
        num_partitions,
        **kwargs,
    )


def insert_cols_bmark(shape, ratio, num_partitions=0, **kwargs):
    return _run_mutation_benchmark(
        insert.insert_bench,
        _configure_cols,
        insert.Insert,
        shape,
        ratio,
        "Insert columns benchmark",
        num_partitions,
        **kwargs,
    )


def update_rows_bmark(shape, ratio, num_partitions=0, **kwargs):
    return _run_mutation_benchmark(
        update.update_bench,
        _configure_rows,
        update.Update,
        shape,
        ratio,
        "Update rows benchmark",
        num_partitions,
        **kwargs,
    )


def update_cols_bmark(shape, ratio, num_partitions=0, **kwargs):
    return _run_mutation_benchmark(
        update.update_bench,
        _configure_cols,
        update.Update,
        shape,
        ratio,
        "Update columns benchmark",
        num_partitions,
        **kwargs,
    )


def drop_rows_bmark(shape, ratio, num_partitions=0, **kwargs):
    return _run_mutation_benchmark(
        drop.drop_bench,
        _configure_rows,
        drop.Drop,
        shape,
        ratio,
        "Drop rows benchmark",
        num_partitions,
        **kwargs,
    )


def drop_cols_bmark(shape, ratio, num_partitions=0, **kwargs):
    return _run_mutation_benchmark(
        drop.drop_bench,
        _configure_cols,
        drop.Drop,
        shape,
        ratio,
        "Drop columns benchmark",
        num_partitions,
        **kwargs,
    )


def astype_bmark(shape, ratio, num_partitions=0, **kwargs):
    cols = np.random.choice(range(shape[1]), round(shape[1] * ratio), replace=False)
    cols_to_change_dtype = [f"c{n}" for n in cols]

    for dtype, to in ASTYPE_CONVERSIONS:
        astype.Astype(
            shape,
            cols_to_change_dtype,
            dtype=dtype,
            to=to,
            num_partitions=num_partitions,
        )

    return astype.astype_bench.run(
        table_header("Change dtype benchmark", shape),
        **kwargs,
    )


def _configure_rows(module, shape, ratio, **kwargs):
    start = round(shape[0] * ratio)
    stop = start * 2
    after = shape[0] - start
    rows_list = list(range(start, stop))

    module(shape=shape, rows={"before": start}, name=f"(before {start:,d})", **kwargs)
    module(shape=shape, rows={"after": after}, name=f"(after {after:,d})", **kwargs)
    module(
        shape=shape,
        rows={"between": [start, stop]},
        name=f"(between {start:,d}-{stop:,d})",
        **kwargs,
    )
    module(
        shape=shape,
        rows=rows_list,
        name=f"(list of {len(rows_list):,d} rows)",
        **kwargs,
    )


def _configure_cols(module, shape, ratio, **kwargs):
    cols = [f"c{n}" for n in range(shape[1])]
    num_cols = round(len(cols) * ratio)
    cols = np.random.choice(cols, num_cols, replace=False)
    module(shape=shape, cols=cols, name=f"(list of {num_cols:,d} cols)", **kwargs)


BENCHMARKS = {
    "read": [read_bmark, read_rows_bmark, read_cols_bmark],
    "read[full]": [read_bmark],
    "read[rows]": [read_rows_bmark],
    "read[cols]": [read_cols_bmark],
    "write": [write_bmark, write_unsorted_bmark],
    "write[sorted]": [write_bmark],
    "write[unsorted]": [write_unsorted_bmark],
    "append": [append_bmark],
    "insert": [insert_rows_bmark, insert_cols_bmark],
    "insert[rows]": [insert_rows_bmark],
    "insert[cols]": [insert_cols_bmark],
    "update": [update_rows_bmark, update_cols_bmark],
    "update[rows]": [update_rows_bmark],
    "update[cols]": [update_cols_bmark],
    "drop": [drop_rows_bmark, drop_cols_bmark],
    "drop[rows]": [drop_rows_bmark],
    "drop[cols]": [drop_cols_bmark],
    "astype": [astype_bmark],
}


def run_bmarks(shape, num_partitions, ratio=0.25, run="all", **kwargs):
    if run == "all":
        funcs = list(
            dict.fromkeys(
                func for bench_funcs in BENCHMARKS.values() for func in bench_funcs
            )
        )
    else:
        funcs = BENCHMARKS[run]
    for func in funcs:
        func(shape=shape, ratio=ratio, num_partitions=num_partitions, **kwargs)


if __name__ == "__main__":
    run_bmarks(
        run="all",
        shape=(1_000, 12),
        num_partitions=20,
        n=3,
        r=5,
        sort=False,
        quiet=False,
    )
