from collections import UserDict


class CaseInsensitiveDict(UserDict):
    """ map keys regardless of their casing
        NOTE keys _must_ be strings
        TODO int support

        UserDict source code
        https://github.com/python/cpython/blob/master/Lib/collections/__init__.py
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._used_keys = set()

    def __setitem__(self, key, value):
        if not isinstance(key, str):
            raise ValueError("key must be a string")
        super().__setitem__(key.lower(), value)

    def __getitem__(self, key):
        if not isinstance(key, str):
            raise ValueError("key must be a string")
        _key = key.lower()
        self._used_keys.add(_key)
        return super().__getitem__(_key)

    def __delitem__(self, key):
        _key = key.lower()
        super().__delitem__(_key)
        self._used_keys.discard(_key)  # doesn't raise if missing

    def __contains__(self, key):
        return super().__contains__(key.lower())

    def get(self, key, type=None, default=None, nullarg_ok=True):
        # NOTE this overrides the builtin type function to match the functionality
        #   of werkzeug.datastructures.MultiDict, which this attempts to work with
        #   to permit logic to receive both URL:GET and JSON:POST args
        try:
            value = self.__getitem__(key)
            if type is None:
                return value
            if value is None and nullarg_ok:
                return None
            return type(value)
        except KeyError:
            if default is not None:
                return type(default) if type is not None else default
            return None

    def keys(self):
        for _key in super().keys():
            self._used_keys.add(_key)
            yield _key

    def unused_keys(self):
        keys_used = self._used_keys.copy()  # prevent mutation when listing keys
        keys_unused = set(self.keys()) - keys_used
        self._used_keys = keys_used  # restore the set to its original content
        return keys_unused
