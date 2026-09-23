"""
Require docstrings and standalone quote lines alongside pydoclint's section checks.
"""

from __future__ import annotations

import argparse
import ast
from pathlib import Path
import re
import sys
import tomllib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Iterator


ROOT = Path(__file__).resolve().parents[2]


def python_files(paths: list[Path], exclude: re.Pattern[str]) -> Iterator[Path]:
    """
    Find handwritten Python files using the same exclusions as pydoclint.

    Args:
        paths (list[Path]): Files and directories requested by the caller.
        exclude (re.Pattern[str]): Generated-file exclusion pattern.

    Yields:
        Path: Python source file that has not already been visited.
    """
    seen = set()
    for path in paths:
        candidates = path.rglob('*.py') if path.is_dir() else [path]
        for candidate in candidates:
            if candidate.suffix != '.py' or exclude.search(str(candidate)):
                continue
            resolved = candidate.resolve()
            if resolved not in seen:
                seen.add(resolved)
                yield candidate


def check(path: Path) -> list[str]:
    """
    Check docstring presence and layout without importing application code.

    Args:
        path (Path): Python source file to inspect.

    Returns:
        list[str]: Diagnostics containing the file, line, and violated requirement.
    """
    source = path.read_text(encoding='utf-8')
    tree = ast.parse(source, filename=str(path))
    errors = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        name = getattr(node, 'name', '<module>')
        docstring = ast.get_docstring(node)
        if docstring is None or not docstring.strip():
            if not isinstance(node, ast.Module):
                errors.append(f'{path}:{node.lineno}: {name} needs a docstring')
            continue
        expression = node.body[0]
        literal = ast.get_source_segment(source, expression)
        assert literal is not None
        lines = literal.splitlines()
        if len(lines) < 3 or lines[0].strip() != '"""' or lines[-1].strip() != '"""':
            errors.append(f'{path}:{expression.lineno}: {name} must put triple double quotes on separate lines')
    return errors


def main() -> int:
    """
    Report missing or collapsed docstrings for the requested source paths.

    Returns:
        int: Zero when all files pass, or one when any diagnostic is reported.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('paths', nargs='*', type=Path, default=[ROOT / 'pkg', ROOT / 'scripts', ROOT / 'integrations'])
    options = parser.parse_args()
    with (ROOT / 'pyproject.toml').open('rb') as config:
        exclude = re.compile(tomllib.load(config)['tool']['pydoclint']['exclude'])
    diagnostics = [error for path in python_files(options.paths, exclude) for error in check(path)]
    for diagnostic in diagnostics:
        print(diagnostic, file=sys.stderr)
    return int(bool(diagnostics))


if __name__ == '__main__':
    sys.exit(main())
