import logging

from pyspark import Row

__author__ = 'ThucNC'
_logger = logging.getLogger(__name__)


def to_dict(self: Row, recursive=True, sort_keys=False):
    """
    Convert current Row into a dictionary, similar to Row.asDict but `recursive` is set to True by default.

    Note: since Python 3.7, dictionaries are ordered
    Args:
        self:
        recursive: convert nested Row also
        sort_keys: sort keys of result dictionary, this works for Python 3.7+ only

    Returns:

    """
    res = self.asDict(recursive=recursive)

    if sort_keys:
        res = {k: res[k] for k in sorted(res.keys())}

    return res


def to_tuple(self: Row):
    """
    Convert current Row data into a tuple

    Args:
        self:

    Returns:

    """
    data = self.asDict()
    return tuple(data.values())


Row.to_dict = to_dict
Row.to_tuple = to_tuple
