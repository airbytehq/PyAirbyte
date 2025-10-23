#!/usr/bin/env python3
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Automated pytest slow marker tuner for PyAirbyte.

This tool runs all tests with a timeout throttle and automatically adds or removes
the @pytest.mark.slow decorator based on test execution time.

Usage:
    python bin/tune_slow_markers.py [--timeout SECONDS] [--dry-run] [--remove-slow]
    pipx run airbyte tune-slow-markers [--timeout SECONDS] [--dry-run] [--remove-slow]
"""

import argparse
import ast
import re
import subprocess
import sys
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Automatically tune pytest slow markers based on test execution time"
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=7.0,
        help="Timeout threshold in seconds (default: 7.0)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be changed without modifying files",
    )
    parser.add_argument(
        "--remove-slow",
        action="store_true",
        help="Also remove slow markers from tests that finish within the threshold",
    )
    parser.add_argument(
        "--test-path",
        type=str,
        default="tests/",
        help="Path to test directory (default: tests/)",
    )
    return parser.parse_args()


def run_pytest_with_durations(test_path: str) -> tuple[list[dict[str, any]], int]:
    """Run pytest with durations reporting to get timing information for all tests.

    Returns:
        Tuple of (list of test results with timing, return code)
    """
    cmd = [
        "poetry",
        "run",
        "pytest",
        test_path,
        "--durations=0",
        "--tb=no",
        "-v",
        "--collect-only",
        "-q",
    ]

    print(f"Collecting tests from {test_path}...")
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)

    if result.returncode not in (0, 5):
        print(f"Warning: Test collection returned code {result.returncode}")

    cmd = [
        "poetry",
        "run",
        "pytest",
        test_path,
        "--durations=0",
        "--tb=line",
        "-v",
        "-m",
        "not super_slow",
    ]

    print(f"Running tests from {test_path} to collect timing data...")
    print("This may take a while...")
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)

    test_timings = []
    duration_pattern = re.compile(r"^([\d.]+)s\s+(?:call|setup|teardown)\s+(.+)$")

    for line in result.stdout.split("\n"):
        match = duration_pattern.match(line.strip())
        if match:
            duration = float(match.group(1))
            test_name = match.group(2)
            test_timings.append({"test": test_name, "duration": duration})

    return test_timings, result.returncode


def parse_test_name(test_name: str) -> tuple[str, str]:
    """Parse pytest test name to extract file path and test function name.

    Example: 'tests/unit_tests/test_foo.py::test_bar' -> ('tests/unit_tests/test_foo.py', 'test_bar')
    """
    if "::" in test_name:
        parts = test_name.split("::")
        file_path = parts[0]
        test_func = parts[-1]
        if "[" in test_func:
            test_func = test_func.split("[")[0]
        return file_path, test_func
    return "", ""


def find_slow_tests(test_timings: list[dict[str, any]], threshold: float) -> set[tuple[str, str]]:
    """Identify tests that exceed the timeout threshold.

    Returns:
        Set of (file_path, test_function_name) tuples
    """
    slow_tests = set()

    for timing in test_timings:
        if timing["duration"] > threshold:
            file_path, test_func = parse_test_name(timing["test"])
            if file_path and test_func:
                slow_tests.add((file_path, test_func))
                print(f"  Slow test found: {test_func} in {file_path} ({timing['duration']:.2f}s)")

    return slow_tests


def find_fast_tests(test_timings: list[dict[str, any]], threshold: float) -> set[tuple[str, str]]:
    """Identify tests that finish within the timeout threshold.

    Returns:
        Set of (file_path, test_function_name) tuples
    """
    fast_tests = set()

    for timing in test_timings:
        if timing["duration"] <= threshold:
            file_path, test_func = parse_test_name(timing["test"])
            if file_path and test_func:
                fast_tests.add((file_path, test_func))

    return fast_tests


def has_slow_marker(test_node: ast.FunctionDef) -> bool:
    """Check if a test function has @pytest.mark.slow decorator."""
    for decorator in test_node.decorator_list:
        if isinstance(decorator, ast.Attribute):
            if (
                isinstance(decorator.value, ast.Attribute)
                and isinstance(decorator.value.value, ast.Name)
                and decorator.value.value.id == "pytest"
                and decorator.value.attr == "mark"
                and decorator.attr == "slow"
            ):
                return True
        elif isinstance(decorator, ast.Call):
            if isinstance(decorator.func, ast.Attribute):
                if (
                    isinstance(decorator.func.value, ast.Attribute)
                    and isinstance(decorator.func.value.value, ast.Name)
                    and decorator.func.value.value.id == "pytest"
                    and decorator.func.value.attr == "mark"
                    and decorator.func.attr == "slow"
                ):
                    return True
    return False


def add_slow_marker_to_file(file_path: str, test_functions: set[str], dry_run: bool = False) -> int:
    """Add @pytest.mark.slow decorator to specified test functions in a file.

    Returns:
        Number of markers added
    """
    path = Path(file_path)
    if not path.exists():
        print(f"Warning: File not found: {file_path}")
        return 0

    content = path.read_text()
    lines = content.split("\n")

    try:
        tree = ast.parse(content)
    except SyntaxError as e:
        print(f"Warning: Could not parse {file_path}: {e}")
        return 0

    markers_added = 0
    modifications = []

    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name in test_functions:
            if not has_slow_marker(node):
                lineno = node.lineno - 1
                indent = len(lines[lineno]) - len(lines[lineno].lstrip())
                marker_line = " " * indent + "@pytest.mark.slow"

                modifications.append((lineno, marker_line))
                markers_added += 1

                if dry_run:
                    print(
                        f"  [DRY RUN] Would add @pytest.mark.slow to {node.name} at line {lineno + 1}"
                    )
                else:
                    print(f"  Adding @pytest.mark.slow to {node.name} at line {lineno + 1}")

    if not dry_run and modifications:
        for lineno, marker_line in sorted(modifications, reverse=True):
            lines.insert(lineno, marker_line)

        path.write_text("\n".join(lines))

    return markers_added


def remove_slow_marker_from_file(
    file_path: str, test_functions: set[str], dry_run: bool = False
) -> int:
    """Remove @pytest.mark.slow decorator from specified test functions in a file.

    Returns:
        Number of markers removed
    """
    path = Path(file_path)
    if not path.exists():
        print(f"Warning: File not found: {file_path}")
        return 0

    content = path.read_text()
    lines = content.split("\n")

    try:
        tree = ast.parse(content)
    except SyntaxError as e:
        print(f"Warning: Could not parse {file_path}: {e}")
        return 0

    markers_removed = 0
    lines_to_remove = []

    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name in test_functions:
            if has_slow_marker(node):
                for i in range(max(0, node.lineno - 10), node.lineno):
                    line = lines[i].strip()
                    if line == "@pytest.mark.slow":
                        lines_to_remove.append(i)
                        markers_removed += 1

                        if dry_run:
                            print(
                                f"  [DRY RUN] Would remove @pytest.mark.slow from {node.name} at line {i + 1}"
                            )
                        else:
                            print(f"  Removing @pytest.mark.slow from {node.name} at line {i + 1}")
                        break

    if not dry_run and lines_to_remove:
        for lineno in sorted(lines_to_remove, reverse=True):
            del lines[lineno]

        path.write_text("\n".join(lines))

    return markers_removed


def main() -> int:
    """Main entry point for the slow marker tuner."""
    args = parse_args()

    print("=" * 80)
    print("PyAirbyte Slow Marker Tuner")
    print("=" * 80)
    print(f"Timeout threshold: {args.timeout}s")
    print(f"Test path: {args.test_path}")
    print(f"Dry run: {args.dry_run}")
    print(f"Remove slow markers: {args.remove_slow}")
    print("=" * 80)
    print()

    test_timings, _ = run_pytest_with_durations(args.test_path)

    if not test_timings:
        print("No test timing data collected. Tests may have failed to run.")
        return 1

    print(f"\nCollected timing data for {len(test_timings)} test executions")
    print()

    print(f"Finding tests that exceed {args.timeout}s threshold...")
    slow_tests = find_slow_tests(test_timings, args.timeout)

    print(f"\nFound {len(slow_tests)} slow tests")
    print()

    tests_by_file: dict[str, set[str]] = {}
    for file_path, test_func in slow_tests:
        if file_path not in tests_by_file:
            tests_by_file[file_path] = set()
        tests_by_file[file_path].add(test_func)

    total_added = 0
    for file_path, test_funcs in tests_by_file.items():
        print(f"Processing {file_path}...")
        added = add_slow_marker_to_file(file_path, test_funcs, args.dry_run)
        total_added += added

    print()
    print(f"Total slow markers added: {total_added}")

    if args.remove_slow:
        print()
        print(f"Finding tests that finish within {args.timeout}s threshold...")
        fast_tests = find_fast_tests(test_timings, args.timeout)

        print(f"\nFound {len(fast_tests)} fast tests")
        print()

        fast_tests_by_file: dict[str, set[str]] = {}
        for file_path, test_func in fast_tests:
            if file_path not in fast_tests_by_file:
                fast_tests_by_file[file_path] = set()
            fast_tests_by_file[file_path].add(test_func)

        total_removed = 0
        for file_path, test_funcs in fast_tests_by_file.items():
            print(f"Processing {file_path}...")
            removed = remove_slow_marker_from_file(file_path, test_funcs, args.dry_run)
            total_removed += removed

        print()
        print(f"Total slow markers removed: {total_removed}")

    print()
    print("=" * 80)
    if args.dry_run:
        print("DRY RUN COMPLETE - No files were modified")
    else:
        print("COMPLETE")
    print("=" * 80)

    return 0


if __name__ == "__main__":
    sys.exit(main())
