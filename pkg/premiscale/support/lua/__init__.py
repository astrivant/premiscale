"""
Load Lua programs from packaged resource files.
"""

from functools import cache
from importlib.resources import files


__all__ = ['load_lua']


@cache
def load_lua(name: str) -> str:  # noqa: DOC502
    """
    Read a Lua program independently of the process working directory.

    Callers supply Redis keys and arguments separately from the program source.

    Args:
        name (str): Resource path relative to premiscale.support.lua, including the .lua suffix.

    Returns:
        str: Cached UTF-8 Lua source read from the named package resource.

    Raises:
        FileNotFoundError: If the named Lua resource does not exist.
    """
    return files('premiscale.support.lua').joinpath(name).read_text(encoding='utf-8')
