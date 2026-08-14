"""Cast Arrow table column dtypes for tests.

Put dtype-change helpers that operate on in-memory Arrow tables here.
"""

import pyarrow as pa

from . import _utils


def change_dtype(df, to, index_name="index", cols=None):
    target_dtype = to_arrow_dtype(to)
    cols_to_cast = cols if cols is not None else df.schema.names

    schema = _replace_column_dtypes(df.schema, target_dtype, cols_to_cast, index_name)
    df = df.cast(schema)
    return _drop_pandas_metadata(df)


def cast_timestamp_index(df, *, unit=None, tz=None):
    index_name = _utils.get_index_name(df)
    current_type = df.field(index_name).type
    if unit is None:
        unit = current_type.unit
    if tz is None:
        tz = current_type.tz
    col_idx = df.column_names.index(index_name)
    return df.set_column(
        col_idx, index_name, df[index_name].cast(pa.timestamp(unit, tz=tz))
    )


def _replace_column_dtypes(schema, dtype, cols, index_name):
    for idx, field in enumerate(schema):
        if field.name not in cols or field.name == index_name:
            continue
        schema = schema.set(idx, field.with_type(dtype))
    return schema


def _drop_pandas_metadata(df):
    """Casting does not refresh pandas metadata; drop it so conversions follow Arrow types."""
    metadata = df.schema.metadata
    if not metadata or b"pandas" not in metadata:
        return df

    metadata = {key: value for key, value in metadata.items() if key != b"pandas"}
    return df.replace_schema_metadata(metadata or None)


def to_arrow_dtype(dtype):
    try:
        return pa.from_numpy_dtype(dtype)
    except (TypeError, ValueError, pa.ArrowInvalid):
        return dtype
