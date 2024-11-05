# tests/README.md
# Data Tests Overview

## Schema Tests
- Not null validations
- Unique constraints
- Positive value checks

## Custom Tests
- positive_values: Ensures numeric columns have positive values
- daily_metric_consistency: Checks for suspicious daily changes

## Singular Tests
- assert_transfer_amounts_match: Validates transfer amount aggregations
- assert_dex_volumes_match: Validates DEX volume aggregations

## Running Tests
```bash
# Run all tests
dbt test

# Run specific test types
dbt test --select test_type:schema
dbt test --select test_type:singular

# Run tests for specific models
dbt test --select daily_transfers