# Test Timeout Configuration

This document explains PyAirbyte's test timeout configuration to prevent CI timeouts, particularly on Windows.

## Current Configuration

- **Global test timeout**: 600 seconds (10 minutes) per test
- **CI job timeout**: 60 minutes for pytest jobs
- **pytest-timeout plugin**: v2.4.0 installed

## Timeout Strategy

### Per-Test Timeouts
- **Unit tests**: 60 seconds (fast execution expected)
- **Integration tests**: 180 seconds (3 minutes for data operations)
- **Slow tests**: Use existing 600 second global timeout

### Session Timeouts
- **Full test suite**: 3600 seconds (1 hour maximum)
- **Windows CI**: Limited to unit tests only to prevent timeouts

## Usage Examples

```bash
# Run tests with custom per-test timeout
pytest --timeout=120 tests/unit_tests/

# Run integration tests with timeout and duration reporting
poetry run poe test-integration-timeout

# Analyze slow test patterns
poetry run poe test-slow-analysis
```

## Slow Test Analysis

The following integration tests are marked as slow and may cause Windows CI timeouts:
- source-faker tests with 200-300 record scales
- Tests parametrized across multiple cache types (DuckDB, Postgres, BigQuery, Snowflake)
- Docker-based tests (slower on Windows due to Docker performance)
