import os

import pandas as pd
import polars as pl
import pyarrow as pa

from featherstore import connection
from featherstore import _metadata
from featherstore._metadata import METADATA_FOLDER_NAME, Metadata
from featherstore._table import common

check_if_connected = connection.current_db


def check_if_table_exists(table_path):
    table_name = table_path.rsplit('/')[-1]
    if not os.path.exists(table_path):
        raise FileNotFoundError(f"Table {table_name} doesn't exist")


def check_if_store_exists(store_name):
    store_path = os.path.join(connection.current_db(), store_name)
    if not os.path.exists(store_path):
        raise FileNotFoundError(f"Store doesn't exists: '{store_name}'")


def check_if_table_already_exists(table_path):
    table_name = table_path.rsplit('/')[-1]
    if os.path.exists(table_path):
        raise OSError(f"A table with name {table_name} already exists")


def check_if_table_name_is_str(table_name):
    if not isinstance(table_name, str):
        raise TypeError(
            f"'table_name' must be a str, is type {type(table_name)}")


def check_if_store_name_is_str(store_name):
    if not isinstance(store_name, str):
        raise TypeError(f"'store_name' must be a str, is type {type(store_name)}")


def check_if_table_name_is_forbidden(table_name):
    if table_name == METADATA_FOLDER_NAME:
        raise ValueError(f"Table name {METADATA_FOLDER_NAME} is forbidden")


def check_if_table_is_correct_dtype(df):
    if not isinstance(df, (pd.DataFrame, pd.Series, pl.DataFrame, pa.Table)):
        raise TypeError(f"'df' must be a DataFrame (is type {type(df)})")


def check_if_columns_match(df, table_path):
    stored_data_cols = Metadata(table_path, "table")["columns"]
    has_default_index = Metadata(table_path, "table")["has_default_index"]
    append_data_cols = common._get_cols(df, has_default_index)

    if sorted(append_data_cols) != sorted(stored_data_cols):
        raise ValueError("New and old columns doesn't match")
