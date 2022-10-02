from collections.abc import MappingView
from itertools import chain

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
            self._keys = self._set_list(items)

    def set_values(self, items):
        if items is None:
            self._values = None
        else:
            self._values = self._set_list(items)

    def _set_list(self, items):
        if self.items_are_collections(items):
            items = chain(*items)

        if isinstance(items, list):
            _values = items
        elif hasattr(items, 'tolist'):
            _values = items.tolist()
        elif hasattr(items, 'to_list'):
            _values = items.to_list()
        else:
            _values = list(items)
        return _values

    def items_are_collections(self, items):
        try:
            if isinstance(items, MappingView):
                item = next(iter(items))
            elif isinstance(items, (pd.Series, pd.DataFrame)):
                item = items.index[0]
            else:
                item = items[0]
        except Exception:
            item = None
        return _table_utils.is_collection(item)

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
        return tuple(zip(self.keys(), self.values()))

    def append(self, value):
        self.values().append(value)

    def copy(self):
        cls = self.__class__
        new = cls(None)
        new._keys = self.keys()
        new._values = self.values()
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
            rows = self._convert_rows(self.values(), to)
            formatted_rows.set_values(rows)
        return formatted_rows

    def _convert_rows(self, rows, to):
        if _table_utils.typestring_is_temporal(to):
            if len(rows) >= 2_000:  # Approx. threshold for when Pandas is faster
                rows = pd.to_datetime(rows).tolist()
            else:
                rows = list(map(pd.to_datetime, rows))
        elif _table_utils.typestring_is_string(to):
            rows = list(map(str, rows))
        elif _table_utils.typestring_is_int(to):
            rows = list(map(int, rows))
        return rows


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
