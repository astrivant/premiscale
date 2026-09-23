"""
Normalize generated protobuf imports and docstring layout for the Python package.
"""

import argparse
import ast
from pathlib import Path
import re


def normalize(path: Path) -> None:  # noqa: DOC502
    """
    Use package-relative imports and standalone docstring quote lines.

    Args:
        path (Path): Generated Python protobuf or gRPC binding to update.

    Returns:
        None: No value is returned.

    Raises:
        SyntaxError: If the generated binding cannot be parsed as Python.
    """
    text = path.read_text(encoding='utf-8')
    for module in ('externalgrpc', 'templates'):
        text = re.sub(rf'(?m)^import {module}_pb2 as ', f'from . import {module}_pb2 as ', text)
    lines = text.splitlines(keepends=True)
    edits = []
    for node in ast.walk(ast.parse(text)):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            doc = ast.get_docstring(node)
            if doc is None:
                continue
            value = node.body[0]
            literal = ast.get_source_segment(text, value)
            if literal is not None:
                quote_lines = literal.splitlines()
                if len(quote_lines) >= 3 and quote_lines[0].strip() == quote_lines[-1].strip() == '"""':
                    continue
            assert value.end_lineno is not None and value.end_col_offset is not None
            indent = ' ' * value.col_offset
            escaped = doc.replace('\\', '\\\\').replace('"""', '\\"\\"\\"')
            replacement = '"""\n' + '\n'.join(indent + line if line else '' for line in escaped.splitlines()) + '\n' + indent + '"""'
            start = sum(map(len, lines[:value.lineno - 1])) + value.col_offset
            end = sum(map(len, lines[:value.end_lineno - 1])) + value.end_col_offset
            edits.append((start, end, replacement))
    for start, end, replacement in sorted(edits, reverse=True):
        text = text[:start] + replacement + text[end:]
    path.write_text('\n'.join(line.rstrip() for line in text.splitlines()) + '\n', encoding='utf-8')


def main() -> None:
    """
    Normalize each Python binding in the requested output directory.

    Returns:
        None: No value is returned.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    options = parser.parse_args()
    for path in sorted(options.directory.glob('*.py')):
        normalize(path)


if __name__ == '__main__':
    main()
