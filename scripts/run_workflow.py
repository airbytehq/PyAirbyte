# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Script to run GitHub Actions workflows locally using wrkflw."""

import subprocess
import sys
from pathlib import Path


EXPECTED_ARGS = 2


def main() -> None:
    """Run a GitHub Actions workflow locally using wrkflw."""
    if len(sys.argv) != EXPECTED_ARGS:
        print("Usage: poe run-workflow <workflow-filename>")
        print("Example: poe run-workflow python_lint.yml")
        sys.exit(1)

    workflow_file = sys.argv[1]

    if not workflow_file.endswith(".yml"):
        workflow_file += ".yml"

    workflow_path = Path(".github/workflows") / workflow_file

    if not workflow_path.exists():
        print(f"Error: Workflow file {workflow_path} does not exist")
        print("Available workflows:")
        workflows_dir = Path(".github/workflows")
        if workflows_dir.exists():
            for yml_file in workflows_dir.glob("*.yml"):
                print(f"  {yml_file.name}")
        sys.exit(1)

    cmd = ["wrkflw", "run", str(workflow_path), "--runtime", "emulation"]

    print(f"Running: {' '.join(cmd)}")
    try:
        result = subprocess.run(cmd, check=True)
        sys.exit(result.returncode)
    except subprocess.CalledProcessError as e:
        print(f"Error running workflow: {e}")
        sys.exit(e.returncode)
    except FileNotFoundError:
        print("Error: wrkflw command not found. Please ensure wrkflw is installed.")
        sys.exit(1)


if __name__ == "__main__":
    main()
