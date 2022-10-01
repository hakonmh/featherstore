from collections.abc import Collection
import pandas as pd
from featherstore._utils import filter_items_like_pattern
from featherstore._table import _table_utils


class Indexer:

    def __init__(self, items, keywords):
        if hasattr(items, 'copy'):
            items = items.copy()

        if isinstance(items, dict):
            keys = items.keys()
            values = items.values()
        else:
            keys = None
            values = items

        self.set_keys(keys)
        self.set_values(values)
        self.set_keyword(keywords)

    def set_keys(self, items):
        if items is None:
            self._keys = None
        else:
            self._keys = list(items)

    def set_values(self, items):
        if items is None:
            self._values = None
        else:
            self._values = list(items)
            if self._items_are_sequences(self._values):
                self._values = [i for sub_list in self._values for i in sub_list]

    def _items_are_sequences(self, items):
        temp = []
        for item in items:
            is_collection = isinstance(item, Collection)
            is_not_string = not isinstance(item, str)
            temp.append(is_collection and is_not_string)
        return all(temp)

    def set_keyword(self, keywords):
        keyword = None
        if self.keys():
            key = self.keys()[0]
            if isinstance(key, str):
                is_keyword = key.lower() in keywords
                if is_keyword:
                    keyword = key.lower()
        self.keyword = keyword

    def keys(self):
        return self._keys

    def values(self):
        return self._values

    def items(self):
        return list(zip(self.keys(), self.values()))

    def append(self, value):
        self.values().append(value)

    def copy(self):
        cls = self.__class__
        new = cls(None)
        new.set_keys(self.keys())
        new.set_values(self.values())
        new.keyword = self.keyword
        return new

    def __getitem__(self, index):
        if self.keys() is not None:
            if index in self.keys():
                index = self.keys().index(index)
        if self.values() is not None:
            return self.values()[index]

    def __iter__(self):
        if self.values() is None:
            return iter([])
        else:
            return iter(self.values())

    def __len__(self):
        if self.values() is None:
            raise 0
        else:
            return len(self.values())

    def __contains__(self, item):
        if self.values() is None:
            return False
        else:
            return item in self.values()

    def __bool__(self):
        return bool(self.keys()) or bool(self.values())

    def __repr__(self):
        class_name = str(self.__class__)
        class_name = class_name.split('.')[-1]
        class_name = class_name.replace("'>", '')
        return f"{class_name}(Keys: {self.keys()} | Values: {self.values()} |" \
               f" Keyword: {self.keyword})"


class RowIndexer(Indexer):

    def __init__(self, rows):
        super().__init__(rows, keywords=('before', 'after', 'between'))

    def convert_types(self, *, to):
        formatted_rows = self.copy()
        if formatted_rows:
            rows = (self._convert_row(item, to) for item in formatted_rows)
            formatted_rows.set_values(rows)
        return formatted_rows

    def _convert_row(self, row, to):
        if _table_utils.typestring_is_temporal(to):
            row = pd.to_datetime(row)
        elif _table_utils.typestring_is_string(to):
            row = str(row)
        elif _table_utils.typestring_is_int(to):
            row = int(row)
        return row


class ColIndexer(Indexer):

    def __init__(self, cols):
        super().__init__(cols, keywords=['like'])

    def like(self, stored_cols):
        if self.keyword:
            pattern = self._get_pattern()
            cols = filter_items_like_pattern(stored_cols, like=pattern)
        elif self.values() is None:
            cols = stored_cols
        else:
            cols = self.values()
        return ColIndexer(cols)

    def _get_pattern(self):
        pattern = self[0]
        if not isinstance(pattern, str):
            pattern = pattern[0]
        return pattern
