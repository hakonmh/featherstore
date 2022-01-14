from featherstore.connection import (
    connect,
    disconnect,
    create_database,
    current_db
)
from featherstore.store import (
    create_store,
    rename_store,
    drop_store,
    list_stores,
    Store,
)
from featherstore.table import Table

__version__ = "0.0.5"
__all__ = ["connect", "disconnect", "create_database", "current_db",
           "create_store", "rename_store", "drop_store", "list_stores",
           "Store", "Table"]
