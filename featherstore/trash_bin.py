class TrashBin:
    def __init__(self, connection_str):
        raise NotImplementedError

    def list_items(self, like):
        raise NotImplementedError

    def restore_item(self):
        raise NotImplementedError

    def delete_item(self, item_name):
        raise NotImplementedError

    def empty_bin(self):
        raise NotImplementedError

    def change_retention_time(self, new_retention_time):
        raise NotImplementedError

    def deactivate_bin(self):
        raise NotImplementedError

    def activate_bin(self):
        raise NotImplementedError


def _create_bin(self, retention_time):
    raise NotImplementedError
