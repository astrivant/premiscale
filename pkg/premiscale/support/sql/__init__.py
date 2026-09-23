"""
Load SQL statements from packaged resource files.
"""

from functools import cache
from importlib.resources import files


__all__ = ['load_sql']


@cache
def load_sql(name: str) -> str:  # noqa: DOC502
    """
    Read a statement relative to this package, independent of the working directory.

    Callers bind parameters through the database driver and own their transactions.

    Args:
        name (str): SQL resource path relative to premiscale.support.sql, including the .sql suffix.

    Returns:
        str: UTF-8 SQL statement read from the named package resource.

    Raises:
        FileNotFoundError: If the named SQL resource does not exist.
    """
    return files('premiscale.support.sql').joinpath(name).read_text(encoding='utf-8')
