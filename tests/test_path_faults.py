"""Enforce spelling-robust matching in path-sensitive fault injectors.

The rule is deliberately syntactic and conservative: a function supplied to
``patch`` as a replacement or ``side_effect`` may not use raw ``==``/``!=``
when either operand is a full-path expression (a ``Path`` value, ``/`` join,
layout path, resolution, or ``os.fspath`` conversion). Use
``ResolvedPathFault`` and assert its ``interceptions`` count instead. Explicit
``.name`` comparisons remain allowed because some descriptor-relative seams
intentionally select one bare directory-entry name rather than a full path.
"""

from __future__ import annotations

import ast
from pathlib import Path
import unittest


_CALLBACK_KEYWORDS = frozenset({"new", "side_effect"})


def _patch_callback_names(call: ast.Call) -> set[str]:
    positional_start: int | None = None
    if isinstance(call.func, ast.Name) and call.func.id == "patch":
        positional_start = 1
    elif isinstance(call.func, ast.Attribute) and call.func.attr == "patch":
        positional_start = 1
    elif (
        isinstance(call.func, ast.Attribute)
        and call.func.attr == "object"
        and isinstance(call.func.value, ast.Name)
        and call.func.value.id == "patch"
    ):
        positional_start = 2
    if positional_start is None:
        return set()

    names: set[str] = set()
    if len(call.args) > positional_start:
        replacement = call.args[positional_start]
        if isinstance(replacement, ast.Name):
            names.add(replacement.id)
    names.update(
        keyword.value.id
        for keyword in call.keywords
        if keyword.arg in _CALLBACK_KEYWORDS and isinstance(keyword.value, ast.Name)
    )
    return names


def _nearest_function(
    node: ast.AST,
    parents: dict[ast.AST, ast.AST],
) -> ast.FunctionDef | ast.AsyncFunctionDef | None:
    current = parents.get(node)
    while current is not None:
        if isinstance(current, (ast.FunctionDef, ast.AsyncFunctionDef)):
            return current
        current = parents.get(current)
    return None


def _callback_definitions(
    tree: ast.Module,
) -> list[tuple[ast.FunctionDef | ast.AsyncFunctionDef, ast.AST]]:
    parents = {
        child: parent
        for parent in ast.walk(tree)
        for child in ast.iter_child_nodes(parent)
    }
    callbacks: list[tuple[ast.FunctionDef | ast.AsyncFunctionDef, ast.AST]] = []
    for call in (node for node in ast.walk(tree) if isinstance(node, ast.Call)):
        names = _patch_callback_names(call)
        if not names:
            continue
        scope: ast.AST = _nearest_function(call, parents) or tree
        for candidate in ast.walk(scope):
            if not isinstance(candidate, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            if candidate.name not in names or _nearest_function(candidate, parents) is not scope:
                continue
            callbacks.append((candidate, scope))
    return callbacks


def _is_explicit_basename(expression: ast.AST) -> bool:
    return isinstance(expression, ast.Attribute) and expression.attr == "name"


def _is_allowed_basename_comparison(operands: list[ast.AST]) -> bool:
    return any(_is_explicit_basename(operand) for operand in operands) and all(
        _is_explicit_basename(operand)
        or isinstance(operand, (ast.Constant, ast.Name, ast.Subscript))
        for operand in operands
    )


def _is_path_expression(expression: ast.AST, path_names: set[str]) -> bool:
    if _is_explicit_basename(expression):
        return False
    if isinstance(expression, ast.Name):
        return expression.id in path_names
    if isinstance(expression, ast.BinOp) and isinstance(expression.op, ast.Div):
        return True
    if isinstance(expression, ast.Attribute):
        if expression.attr.endswith(("_path", "_root")):
            return True
        if isinstance(expression.value, ast.Name) and expression.value.id == "layout":
            return True
        return _is_path_expression(expression.value, path_names)
    if isinstance(expression, ast.Call):
        function_name = expression.func.id if isinstance(expression.func, ast.Name) else None
        attribute_name = expression.func.attr if isinstance(expression.func, ast.Attribute) else None
        if function_name == "Path" or attribute_name in {"fspath", "resolve", "absolute"}:
            return True
        if function_name == "str" and expression.args:
            return _is_path_expression(expression.args[0], path_names)
    return False


def _path_names(
    callback: ast.FunctionDef | ast.AsyncFunctionDef,
    scope: ast.AST,
) -> set[str]:
    names = {
        argument.arg
        for argument in (*callback.args.posonlyargs, *callback.args.args, *callback.args.kwonlyargs)
        if argument.annotation is not None
        and ("Path" in ast.unparse(argument.annotation) or "PathLike" in ast.unparse(argument.annotation))
    }
    assignments = [
        node
        for node in ast.walk(scope)
        if isinstance(node, (ast.Assign, ast.AnnAssign))
    ]
    changed = True
    while changed:
        changed = False
        for assignment in assignments:
            value = assignment.value
            if value is None or not _is_path_expression(value, names):
                continue
            targets = assignment.targets if isinstance(assignment, ast.Assign) else [assignment.target]
            for target in targets:
                if isinstance(target, ast.Name) and target.id not in names:
                    names.add(target.id)
                    changed = True
    return names


def raw_path_equality_injectors(source: str, filename: str) -> list[str]:
    """Return diagnostics for the documented raw-equality injector rule."""

    tree = ast.parse(source, filename=filename)
    findings: set[tuple[int, str]] = set()
    for callback, scope in _callback_definitions(tree):
        path_names = _path_names(callback, scope)
        for comparison in (
            node for node in ast.walk(callback) if isinstance(node, ast.Compare)
        ):
            if not any(isinstance(operator, (ast.Eq, ast.NotEq)) for operator in comparison.ops):
                continue
            operands = [comparison.left, *comparison.comparators]
            if _is_allowed_basename_comparison(operands):
                continue
            if any(_is_path_expression(operand, path_names) for operand in operands):
                findings.add((comparison.lineno, callback.name))
    return [
        f"{filename}:{line}: {name} uses raw path equality; use ResolvedPathFault and assert interceptions"
        for line, name in sorted(findings)
    ]


class PathFaultGuardTests(unittest.TestCase):
    def test_guard_recognizes_a_raw_path_equality_injector(self) -> None:
        source = """
from pathlib import Path
from unittest.mock import patch

def exercise(layout):
    def unavailable(path: Path):
        if path == layout.source_root / "pyproject.toml":
            raise PermissionError
    with patch("package.reader", side_effect=unavailable):
        pass
"""
        self.assertEqual(
            raw_path_equality_injectors(source, "synthetic_test.py"),
            [
                "synthetic_test.py:7: unavailable uses raw path equality; "
                "use ResolvedPathFault and assert interceptions"
            ],
        )

    def test_guard_recognizes_os_fspath_in_a_positional_replacement(self) -> None:
        source = """
import os
from pathlib import Path
from unittest.mock import patch

def exercise():
    target = Path("fixture") / "target"
    def injected(candidate):
        return os.fspath(candidate) == target.name
    with patch("package.stat", injected):
        pass
"""
        self.assertEqual(
            raw_path_equality_injectors(source, "synthetic_test.py"),
            [
                "synthetic_test.py:9: injected uses raw path equality; "
                "use ResolvedPathFault and assert interceptions"
            ],
        )

    def test_test_fault_injectors_use_resolved_path_matching(self) -> None:
        tests_root = Path(__file__).parent
        violations: list[str] = []
        for source_path in sorted(tests_root.rglob("*.py")):
            relative = source_path.relative_to(tests_root.parent)
            violations.extend(
                raw_path_equality_injectors(
                    source_path.read_text(encoding="utf-8"),
                    relative.as_posix(),
                )
            )
        self.assertEqual(
            violations,
            [],
            "path-sensitive test fault injectors must use ResolvedPathFault:\n"
            + "\n".join(violations),
        )


if __name__ == "__main__":
    unittest.main()
