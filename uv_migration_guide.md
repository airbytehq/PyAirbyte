# Migration Guide: Poetry to uv

This guide documents the process of migrating from Poetry to uv as the package manager for Python projects, while maintaining dynamic versioning capabilities and existing task runners.

## Why Migrate to uv?

- **Performance**: uv offers significantly faster dependency resolution and installation
- **PEP Compliance**: Better alignment with PEP 621 (project metadata) and PEP 508 (dependency specifications)
- **Modern Tooling**: Integration with contemporary Python packaging ecosystem
- **Simplified Configuration**: Cleaner project metadata structure

## Prerequisites

1. Python >=3.10,<3.13 installed
2. Git repository with existing Poetry configuration
3. GitHub Actions for CI/CD (if applicable)

## Step-by-Step Migration Process

### 1. Update Build System Configuration

Replace the existing build-system configuration in `pyproject.toml`:

```toml
# Old configuration
[build-system]
requires = ["poetry>=1.2.0", "poetry-dynamic-versioning>=0.20.0"]
build-backend = "poetry.core.masonry.api"

# New configuration
[build-system]
requires = ["hatchling", "uv-dynamic-versioning"]
build-backend = "hatchling.build"
```

### 2. Migrate Project Metadata

Convert the `[tool.poetry]` section to `[project]` following PEP 621:

```toml
# Old configuration
[tool.poetry]
name = "your-package"
version = "0.0.0"
description = "Your package description"
authors = ["Your Name <your.email@example.com>"]

# New configuration
[project]
name = "your-package"
description = "Your package description"
readme = "README.md"
requires-python = ">=3.10,<3.13"
license = "MIT"
dynamic = ["version"]
authors = [
    {name = "Your Name", email = "your.email@example.com"}
]
```

### 3. Update Dependencies Format

Convert dependencies to PEP 508 format:

```toml
# Old format (Poetry)
[tool.poetry.dependencies]
python = "^3.10,<3.13"
requests = "^2.28.0"

# New format (PEP 508)
[project.dependencies]
requests = ">=2.28,<3.0"
```

### 4. Configure Dynamic Versioning

Add uv-dynamic-versioning configuration:

```toml
[tool.hatch.version]
source = "uv-dynamic-versioning"
```

### 5. Update Optional Dependencies

Move optional dependencies to the new format:

```toml
# Old format
[tool.poetry.group.dev.dependencies]
pytest = "^7.0.0"

# New format
[project.optional-dependencies]
dev = [
    "pytest>=7.0,<8.0"
]
```

### 6. Update GitHub Actions

Replace Poetry setup with uv in your GitHub Actions workflows:

```yaml
name: Python Tests

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      
      - name: Set up uv
        uses: astral-sh/setup-uv@v5
        with:
          python-version: "3.12"
          enable-cache: true
      
      - name: Install dependencies
        run: uv install
      
      - name: Run tests
        run: poe test-all
```

## Verification Steps

1. Validate TOML syntax:
   ```bash
   python -m tomli pyproject.toml
   ```

2. Install dependencies with uv:
   ```bash
   uv install
   ```

3. Run existing tests:
   ```bash
   poe test-all
   ```

## Common Issues and Solutions

### Dynamic Versioning

If you encounter version extraction issues:
1. Ensure all commits are properly tagged
2. Verify uv-dynamic-versioning is listed in build-system.requires
3. Check that "version" is listed in project.dynamic

### Dependencies

If you encounter dependency resolution issues:
1. Verify all version constraints follow PEP 508 format
2. Remove any Poetry-specific operators (^, ~)
3. Use explicit version ranges (>=x.y.z, <a.b.c)

## Notes

- Keep existing poethepoet tasks unchanged
- Maintain separation between core dependencies and optional extras
- Follow PEP 508 format for all dependency specifications
- Ensure CI/CD pipelines are updated to use uv instead of Poetry

## References

- [uv Documentation](https://github.com/astral-sh/uv)
- [uv-dynamic-versioning](https://github.com/ninoseki/uv-dynamic-versioning)
- [PEP 621 – Storing project metadata in pyproject.toml](https://peps.python.org/pep-0621/)
- [PEP 508 – Dependency specification for Python Software Packages](https://peps.python.org/pep-0508/)
