from featherstore.connection import (
    connect,
    disconnect,
    create_database,
    current_db,
    is_connected
)
from featherstore.store import (
    create_store,
    rename_store,
    drop_store,
    list_stores,
    store_exists,
    Store,
)
from featherstore.table import Table
from featherstore import snapshot

__version__ = "0.2.1"
__all__ = ["connect", "disconnect", "create_database", "current_db",
           "is_connected", "create_store", "rename_store", "drop_store",
           "list_stores", "store_exists", "Store", "Table", "snapshot"]
