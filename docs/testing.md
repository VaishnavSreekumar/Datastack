# DataStack Testing & Quality Assurance Strategy

## 1. Testing Philosophy

DataStack avoids fragile, brittle tests that rely on external internet connectivity or gigabytes of production data. Instead, it employs:
- **Fast, Deterministic Unit Tests:** Small synthetic fixtures mirroring real Olist edge cases.
- **Data Quality Contracts:** Both generic dbt schema tests and custom singular business logic tests.
- **Financial Reconciliation Assertions:** Guarantees that revenue totals, order counts, and delivery latency metrics are mathematically sound.

---

## 2. Test Architecture

### A. Synthetic Unit Tests (`tests/`)
1. **Schema Validation Tests (`test_schemas.py`):**
   - Asserts that corrupt, missing, or malformed columns are caught at ingestion.
   - Tests date/timestamp parsing across Portuguese formatting and standard ISO-8601 strings.
2. **Join Cardinality & Financial Integrity (`test_joins.py`):**
   - Tests order-to-items (1:N) and order-to-payments (1:M) joins.
   - Proves mathematically that the Cartesian product flaw is eradicated and revenue cannot be multiplied.
3. **Delivery Delay & SLA Calculations (`test_transformations.py`):**
   - Tests on-time, early (negative delay), and late (positive delay) delivery timestamps.
   - Tests edge cases where delivery dates are null (in-flight or processing orders).
4. **Idempotency & Rerun Testing (`test_idempotency.py`):**
   - Simulates pipeline re-execution on existing datasets to verify that data is overwritten/updated rather than duplicated.

### B. dbt Data Quality Tests (`olist_dbt/tests/`)
- `assert_delivered_orders_have_payment.sql`: Every order marked `delivered` must have a non-zero payment recorded.
- `assert_delivery_delay_in_range.sql`: Verifies delivery delay falls within realistic logistics bounds (e.g., `-30` to `365` days).
- `assert_customers_have_spend.sql`: Every customer in customer dimension must reflect positive total spend and at least one order.
- `assert_no_revenue_duplication.sql`: Verifies sum of silver item values equals order-level net value.

---

## 3. Running the Tests

```powershell
# Run the entire test suite
pytest -v

# Run with test coverage report
pytest --cov=datastack tests/

# Run dbt data tests
cd olist_dbt
dbt test
```
