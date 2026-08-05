import featherstore as fs

from . import _fixtures as fx


def partition_size(df, num_partitions):
    return fx.get_partition_size(df, num_partitions)


def open_table():
    fs.create_database("db")
    store = fs.create_store("store_name")
    return store.select_table("table_name")


def close_table():
    fs.drop_store("store_name")
    fx.delete_db()
