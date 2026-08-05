import pyarrow as pa


def change_dtype(df, to, index_name="index", cols=None):
    target_dtype = to_arrow_dtype(to)
    cols_to_cast = cols if cols is not None else df.schema.names

    schema = _replace_column_dtypes(df.schema, target_dtype, cols_to_cast, index_name)
    df = df.cast(schema)
    return _drop_pandas_metadata(df)


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
