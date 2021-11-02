import os
import json

import pyarrow as pa
import pandas as pd
import polars as pl

from ..connection import current_db
from .._metadata import METADATA_FOLDER_NAME
from .._utils import DEFAULT_ARROW_INDEX_NAME


def can_init_table(table_name, store_name):
    current_db()

    if not isinstance(table_name, str):
        raise TypeError(f"'table_name' must be a str, is type {type(table_name)}")

    if table_name == METADATA_FOLDER_NAME:
        raise ValueError(f"Table name {METADATA_FOLDER_NAME} is forbidden")

    if not isinstance(store_name, str):
        raise TypeError(f"{store_name} must be a str")

    store_path = f"{current_db()}/{store_name}"
    if not os.path.exists(store_path):
        raise FileNotFoundError(f"Store doesn't exists: '{store_name}'")


def can_rename_table(new_table_name, old_table_path, new_table_path):
    if not isinstance(new_table_name, str):
        raise TypeError(f"'new_name' must be a str (is type {type(new_table_name)})")

    if new_table_name == METADATA_FOLDER_NAME:
        raise ValueError(f"Table name {METADATA_FOLDER_NAME} is forbidden")

    if not os.path.exists(old_table_path):
        raise FileNotFoundError("Can not rename a none existing table")

    if os.path.exists(new_table_path):
        raise OSError(f"A table with name {new_table_name} already exists")


def combine_partitions(partitions):
    full_table = pa.concat_tables(partitions)
    return full_table


def format_table(df, index, warning):
    df = _convert_to_pandas(df)
    df = _set_index(df, index)
    _check_index_constraints(df)

    index_is_sorted = df.index.is_monotonic_increasing
    if not index_is_sorted:
        df = _sort_index(df, warning)
        new_metadata = json.dumps({"sorted": True})
    else:
        new_metadata = json.dumps({"sorted": False})

    formatted_df = pa.Table.from_pandas(df, preserve_index=True)
    formatted_df = _add_schema_metadata(formatted_df, new_metadata)
    return formatted_df


def _add_schema_metadata(df, new_metadata):
    old_metadata = df.schema.metadata
    combined_metadata = {**old_metadata, b"featherstore": new_metadata}
    df = df.replace_schema_metadata(combined_metadata)
    return df


def _convert_to_pandas(df):
    if isinstance(df, pd.DataFrame):
        pd_df = df
    elif isinstance(df, pd.Series):
        pd_df = df.to_frame()
    elif isinstance(df, pa.Table):
        pd_df = df.to_pandas()
    elif isinstance(df, pl.DataFrame):
        pd_df = df.to_pandas()
        if DEFAULT_ARROW_INDEX_NAME in pd_df.columns:
            pd_df = pd_df.set_index(DEFAULT_ARROW_INDEX_NAME)
            pd_df.index.name = None
    return pd_df


def _set_index(df, index):
    if index and df.index.name != index and index in df.columns:
        df = df.set_index(index)
    if df.index.name == DEFAULT_ARROW_INDEX_NAME:
        df.index.name = None
    return df


def _check_index_constraints(df):
    index_type = df.index.inferred_type
    if index_type not in {"integer", "datetime64", "string"}:
        raise TypeError("Table.index type must be either int, str or datetime")
    if df.index.has_duplicates:
        raise ValueError("Values in Table.index must be unique")


def _sort_index(df, warning):
    if warning == "warn":
        import warnings

        warnings.warn("Index is unsorted and will be sorted before storage")
    df = df.sort_index()
    return df


def _get_cols(df, has_default_index):
    if isinstance(df, pd.DataFrame):
        cols = df.columns.tolist()
        if df.index.name is not None:
            cols.append(df.index.name)
        else:
            cols.append(DEFAULT_ARROW_INDEX_NAME)
    elif isinstance(df, pd.Series):
        cols = [df.name]
        if df.index.name is not None:
            cols.append(df.index.name)
        else:
            cols.append(DEFAULT_ARROW_INDEX_NAME)
    elif isinstance(df, pa.Table):
        cols = df.column_names
        if has_default_index and DEFAULT_ARROW_INDEX_NAME not in cols:
            cols.append(DEFAULT_ARROW_INDEX_NAME)
    elif isinstance(df, pl.DataFrame):
        cols = df.columns
        if has_default_index and DEFAULT_ARROW_INDEX_NAME not in cols:
            cols.append(DEFAULT_ARROW_INDEX_NAME)
    return cols
