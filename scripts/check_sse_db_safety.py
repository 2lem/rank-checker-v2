from __future__ import annotations

import ast
import sys
from pathlib import Path


STREAMING_RESPONSE_NAMES = {"StreamingResponse", "EventSourceResponse"}
DISALLOWED_PARAM_NAMES = {"db"}
DISALLOWED_NAME_REFERENCES = {"db", "Session", "get_db"}


class SSEDBSafetyChecker(ast.NodeVisitor):
    def __init__(self, filename: str) -> None:
        self.filename = filename
        self.errors: list[str] = []
        self.function_defs: dict[str, ast.FunctionDef] = {}
        self.generator_functions: set[str] = set()

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self.function_defs[node.name] = node
        if any(isinstance(child, (ast.Yield, ast.YieldFrom)) for child in ast.walk(node)):
            self.generator_functions.add(node.name)
        self.generic_visit(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self.function_defs[node.name] = node  # type: ignore[assignment]
        if any(isinstance(child, (ast.Yield, ast.YieldFrom)) for child in ast.walk(node)):
            self.generator_functions.add(node.name)
        self.generic_visit(node)

    def visit_Return(self, node: ast.Return) -> None:
        if isinstance(node.value, ast.Call) and self._is_streaming_response(node.value):
            self._check_streaming_return(node.value)
        self.generic_visit(node)

    def _is_streaming_response(self, call: ast.Call) -> bool:
        name = self._get_call_name(call.func)
        return name in STREAMING_RESPONSE_NAMES

    @staticmethod
    def _get_call_name(node: ast.AST) -> str | None:
        if isinstance(node, ast.Name):
            return node.id
        if isinstance(node, ast.Attribute):
            return node.attr
        return None

    def _check_streaming_return(self, call: ast.Call) -> None:
        enclosing_func = self._find_enclosing_function(call)
        if enclosing_func is None:
            return
        self._check_streaming_route_signature(enclosing_func)
        generator_name = self._extract_generator_name(call)
        if generator_name:
            self._check_generator_function(generator_name)

    def _find_enclosing_function(self, node: ast.AST) -> ast.FunctionDef | None:
        current = node
        while current:
            current = getattr(current, "parent", None)
            if isinstance(current, ast.FunctionDef):
                return current
        return None

    def _check_streaming_route_signature(self, node: ast.FunctionDef) -> None:
        for arg in node.args.args:
            if arg.arg in DISALLOWED_PARAM_NAMES:
                default = self._default_for_arg(node, arg.arg)
                if self._is_depends_get_db(default):
                    self.errors.append(
                        f"{self.filename}:{node.lineno} route '{node.name}' "
                        "uses Depends(get_db) in SSE/streaming response parameters."
                    )

    def _default_for_arg(self, node: ast.FunctionDef, arg_name: str) -> ast.AST | None:
        positional_args = node.args.args
        defaults = node.args.defaults
        default_offset = len(positional_args) - len(defaults)
        for index, arg in enumerate(positional_args):
            if arg.arg == arg_name:
                default_index = index - default_offset
                if default_index >= 0:
                    return defaults[default_index]
        return None

    def _is_depends_get_db(self, node: ast.AST | None) -> bool:
        if not isinstance(node, ast.Call):
            return False
        if self._get_call_name(node.func) != "Depends":
            return False
        if not node.args:
            return False
        return self._get_call_name(node.args[0]) == "get_db"

    def _extract_generator_name(self, call: ast.Call) -> str | None:
        if not call.args:
            return None
        first_arg = call.args[0]
        if isinstance(first_arg, ast.Name):
            return first_arg.id
        return None

    def _check_generator_function(self, name: str) -> None:
        if name not in self.generator_functions:
            return
        generator_node = self.function_defs.get(name)
        if generator_node is None:
            return
        for child in ast.walk(generator_node):
            if isinstance(child, ast.Name) and child.id in DISALLOWED_NAME_REFERENCES:
                self.errors.append(
                    f"{self.filename}:{generator_node.lineno} generator '{name}' "
                    "references DB/session symbols in SSE/streaming context."
                )
                break


def _attach_parents(tree: ast.AST) -> None:
    for parent in ast.walk(tree):
        for child in ast.iter_child_nodes(parent):
            child.parent = parent  # type: ignore[attr-defined]


def check_file(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    _attach_parents(tree)
    checker = SSEDBSafetyChecker(str(path))
    checker.visit(tree)
    return checker.errors


def main() -> int:
    root = Path(__file__).resolve().parents[1] / "app"
    errors: list[str] = []
    for path in root.rglob("*.py"):
        errors.extend(check_file(path))
    if errors:
        print("SSE DB safety check failed:")
        for error in errors:
            print(f" - {error}")
        return 1
    print("SSE DB safety check passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
