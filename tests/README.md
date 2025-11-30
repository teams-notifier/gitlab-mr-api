# Test Suite

## Setup

```bash
uv pip install -r requirements-dev.txt
```

## Running Tests

```bash
# All tests
pytest -v

# Specific test file
pytest -v tests/test_integration_webhook_flow.py
pytest -v tests/test_mr_state_transitions.py
pytest -v tests/test_participant_filtering.py
pytest -v tests/test_merge_request_deletion.py
pytest -v tests/test_periodic_cleanup.py
pytest -v tests/test_message_operations.py
pytest -v tests/test_database_helpers.py

# Specific test
pytest -v tests/test_merge_request_deletion.py::test_merge_close_deletion_no_transaction_causes_orphan

# With coverage
pytest --cov=. --cov-report=html

# Run only bug tests
pytest -v -k "test_merge_request_deletion or test_periodic_cleanup or test_message_operations"

# Run only integration tests
pytest -v -k "test_integration or test_mr_state or test_participant"
```

## Test Organization

### Bug Detection Tests (24 tests)

#### test_merge_request_deletion.py (7 tests)
Tests critical bugs in merge/close deletion logic:
- Non-transactional operations causing orphaned records
- Partial deletion failures
- Multiple message handling
- Race condition error handling
- Draft-to-ready positive test

**Key Bug:** `webhook/merge_request.py:299-327` lacks transaction wrapper

#### test_periodic_cleanup.py (10 tests)
Tests activity-API error handling:
- Network failures
- HTTP errors (500, 429, 410)
- Timeouts
- Database errors
- Missing retry mechanism
- Multiple record failures

**Key Bug:** `periodic_cleanup.py:42-58` no retry on activity-API failures

#### test_message_operations.py (6 tests)
Tests message handling protection mechanisms:
- Merge creates single deletion record
- Update existing message uses PATCH
- Get or create returns existing and new refs
- Duplicate message cleanup on DB conflict
- Draft transition uses FOR UPDATE lock
- Approval updates extra state

**Note:** Real race condition tests are in `test_e2e_race_conditions.py`

---

### Integration & Application Logic Tests (60+ tests)

#### test_integration_webhook_flow.py (13 tests)
Full end-to-end webhook processing tests:
- MR open webhook flow
- MR merge webhook with deletion
- Invalid GitLab tokens
- Invalid conversation tokens
- Multiple conversation tokens (multiple channels)
- Activity-API error propagation
- Healthcheck endpoint
- Root redirect

**Coverage:** FastAPI endpoint → webhook handler → database → activity-API

#### test_mr_state_transitions.py (12 tests)
MR lifecycle state transitions:
- Open/reopen creates message
- Draft renders collapsed
- Merged/closed renders collapsed and triggers deletion
- Approved/unapproved updates approvers
- New commits reset approvals
- Draft-to-ready deletes old messages
- Update uses PATCH, open uses POST

**Coverage:** All MR actions and state changes

#### test_participant_filtering.py (11 tests)
Participant-based message filtering:
- No filter creates message
- Opener/assignee/reviewer in filter creates message
- No participant match uses update_only mode
- Multiple participants
- Filter validation
- Invalid filter format returns 400
- Draft MR with non-participant

**Coverage:** `filter_on_participant_ids` query parameter logic

#### test_database_helpers.py (14 tests)
Database helper function tests:
- GitLab instance ID creation/retrieval
- MR ref infos creation/update
- Generic upsert (insert/update)
- Race condition handling (UniqueViolationError)
- Connection lifecycle
- Acquire context manager

**Coverage:** `db.py` DBHelper class methods

---

## Test Statistics

- **Total Tests:** 132 tests
- **Unit Tests:** ~100 tests
- **E2E Tests:** ~30 tests (with testcontainers)
- **Coverage:** 89% (source only, tests excluded)

## Key Findings

### Critical Bugs (Fix Immediately)
1. ✅ **Transaction wrapper missing** in `webhook/merge_request.py:299-327`
2. ✅ **No retry mechanism** in `periodic_cleanup.py:42-58`
3. ✅ **Race condition error handling** incomplete

### Application Logic Validation
✅ All major application flows validated:
- Webhook endpoint routing
- MR state transitions
- Participant filtering
- Database operations
- Activity-API integration

## Running Specific Test Categories

```bash
# Bug detection tests only
pytest -v tests/test_merge_request_deletion.py tests/test_periodic_cleanup.py tests/test_message_operations.py

# Integration tests only
pytest -v tests/test_integration_webhook_flow.py tests/test_mr_state_transitions.py

# Database tests only
pytest -v tests/test_database_helpers.py

# Quick smoke test (integration + state transitions)
pytest -v tests/test_integration_webhook_flow.py::test_webhook_endpoint_merge_request_open \
         tests/test_mr_state_transitions.py::test_mr_state_open_creates_message
```

## Test Coverage Areas

### ✅ Covered (Unit Tests)
- FastAPI endpoint handling
- Webhook routing and validation
- MR lifecycle (open, update, approve, merge, close)
- Participant filtering
- Message creation/update/deletion
- Database helpers and upserts
- Activity-API integration
- Error handling at app level
- Healthcheck

### ✅ Covered (E2E Tests)
- Real database operations
- Schema constraints and indexes
- Transaction behavior
- JSONB operations
- Full webhook flow with database
- Bug validation with real DB
- Race condition handling
- Cascade deletes

### ⚠️ Limited Coverage
- Card rendering templates (`cards/render.py`) - 37%
- Configuration validation
- Performance/load testing

---

## E2E Tests with Real Database

**31 additional E2E tests** available with real PostgreSQL in Docker!

### Quick Start
```bash
./run_e2e_tests.sh
```

### What's Tested
- Database schema and constraints
- Full webhook flow with real DB
- Bug validation
- Transaction behavior
- JSONB operations

See **`../E2E_TESTS.md`** for complete documentation.

---

## References

- **Bug Analysis:** `../BUGS_ANALYSIS.md`
- **E2E Tests:** `../E2E_TESTS.md`
- **Test Fixtures:** `conftest.py`
- **Configuration:** `../pyproject.toml`
- **Quick Fixes:** `../QUICK_FIX_GUIDE.md`
