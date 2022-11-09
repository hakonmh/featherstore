import featherstore as fs
from featherstore._utils import delete_folder_tree, DB_MARKER_NAME
import os


def write_table(df, db_path):
    store = create_store(db_path, 'store')
    table = store.select_table('table')
    table.write(df)
    return table


def create_store(db_path, store_name):
    fs.create_database(db_path)
    return fs.create_store(store_name)


def delete_db():
    db_path = fs.current_db()
    items = os.listdir(db_path)
    if DB_MARKER_NAME in items:
        delete_folder_tree(db_path, db_path)
    else:
        breakpoint()
        raise RuntimeError("Not a database!")
