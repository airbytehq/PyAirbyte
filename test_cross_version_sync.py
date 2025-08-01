# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""Test cross-version Python functionality using the new sync command syntax.

Tests what happens when requesting different Python versions on different Docker base images.
"""

import json
import subprocess
import time
from pathlib import Path


def run_cross_version_test(
    base_python: str, target_python: str, test_name: str
) -> dict[str, str | bool | float]:
    """Run a cross-version Python test using the new sync command syntax."""
    print(f"\n{'=' * 60}")
    print(f"TEST: {test_name}")
    print(f"Base Image: python:{base_python}-slim")
    print(f"Target Python: {target_python}")
    print(f"{'=' * 60}")

    current_dir = Path.cwd()

    docker_cmd = [
        "docker",
        "run",
        "--rm",
        "-v",
        f"{current_dir}:/workspace",
        "-w",
        "/workspace",
        f"python:{base_python}-slim",
        "bash",
        "-c",
        f"""
        set -e
        echo "=== Environment Setup ==="
        echo "Base Python version:"
        python --version

        echo "Installing system dependencies..."
        apt-get update -qq && apt-get install -y -qq git

        echo "Configuring Git..."
        git config --global --add safe.directory /workspace

        echo "Installing Poetry..."
        pip install -q poetry

        echo "Installing PyAirbyte..."
        poetry install -q

        echo "=== Testing Cross-Version Python with New Sync Command ==="
        echo "Attempting to use Python {target_python} on Python {base_python} image..."

        echo "Running sync with --use-python {target_python}..."
        time poetry run pyab sync --source source-faker \\
            --Sconfig '{{"count": 100}}' \\
            --use-python {target_python} \\
            --destination=airbyte/destination-dev-null:0.6.0

        echo "=== Success! ==="
        """,
    ]

    start_time = time.time()
    try:
        print("Starting Docker test...")
        result = subprocess.run(
            docker_cmd, capture_output=True, text=True, timeout=600, check=False
        )
        end_time = time.time()

        return {
            "success": result.returncode == 0,
            "duration": end_time - start_time,
            "return_code": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
            "base_python": base_python,
            "target_python": target_python,
        }

    except subprocess.TimeoutExpired:
        end_time = time.time()
        return {
            "success": False,
            "duration": end_time - start_time,
            "error": "timeout",
            "base_python": base_python,
            "target_python": target_python,
        }
    except Exception as e:
        end_time = time.time()
        return {
            "success": False,
            "duration": end_time - start_time,
            "error": str(e),
            "base_python": base_python,
            "target_python": target_python,
        }


def main() -> None:
    """Run cross-version Python tests with the new sync command syntax."""
    print("Testing Cross-Version Python Functionality with New Sync Command")

    test_cases = [
        ("3.10", "3.10.3", "Python 3.10.3 on Python 3.10 image"),
        ("3.10", "3.11", "Python 3.11 on Python 3.10 image"),
        ("3.11", "3.10.3", "Python 3.10.3 on Python 3.11 image"),
        ("3.11", "3.12", "Python 3.12 on Python 3.11 image"),
    ]

    results = {}

    for base_python, target_python, test_name in test_cases:
        test_key = f"{base_python}_to_{target_python}"
        results[test_key] = run_cross_version_test(base_python, target_python, test_name)

    results_file = Path("cross_version_sync_results.json")
    results_file.write_text(json.dumps(results, indent=2), encoding="utf-8")

    print(f"\n{'=' * 60}")
    print("CROSS-VERSION SYNC TESTING COMPLETE")
    print(f"{'=' * 60}")
    print(f"Results saved to: {results_file}")

    print("\nSUMMARY:")
    for test_key, result in results.items():
        base, target = test_key.split("_to_")
        status = "✓" if result["success"] else "✗"
        duration = result.get("duration", 0)
        print(f"{status} Python {target} on {base} image: {duration:.1f}s")
        if not result["success"]:
            error = result.get("error", "unknown error")
            print(f"   Error: {error}")


if __name__ == "__main__":
    main()
