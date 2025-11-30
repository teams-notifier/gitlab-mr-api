# GitLab MR API Workflow Documentation

This document describes the expected behavior and message lifecycle for the GitLab merge request webhook integration with Microsoft Teams.

## GitLab Webhook Configuration

### Multiple Webhooks Per Project

A GitLab project can have multiple webhooks registered, each with:
- Different Microsoft Teams channels (`conversation_token`)
- Different filtering criteria (`participant_ids_filter`)
- Independent processing pipelines

### Webhook Behavior

When a merge request event occurs in GitLab:
1. GitLab triggers **all registered webhooks** for that project
2. Each webhook call is **independent** and processes separately
3. Each webhook may target different Teams channels (different `conversation_token` values)
4. A single MR event can generate **multiple messages** in different channels

**Important**: The system is designed to handle multiple webhook calls for the same MR event, creating separate messages per conversation token.

## Message Lifecycle

### 1. MR Open / Update / Reopen

**Actions**: `open`, `reopen`, `update` (with relevant changes like assignee, reviewer, etc.)

**Behavior**:
- **UPDATE existing message** OR **CREATE new message** if missing
- Processes each `conversation_token` independently
- Applies `participant_ids_filter` criteria
- Message shows full MR details (expanded template)

**Code Path**: `webhook/merge_request.py` lines 153-188
- Calls `get_or_create_message_refs()` to get/create message references
- Calls `create_or_update_message()` for each conversation token
- `update_only` parameter determines CREATE vs UPDATE behavior

**Database Protection**:
- Unique constraint on `(merge_request_ref_id, conversation_token)` prevents duplicate messages per channel
- `FOR UPDATE` lock in `get_or_create_message_refs()` prevents concurrent corruption

### 2. MR Switches to Draft

**Actions**: `update` with `draft=true`

**Behavior**:
- **UPDATE all existing messages** with collapsed template
- Icon changes to indicate draft status
- Messages remain visible but less prominent
- No deletion occurs

**Code Path**: `webhook/merge_request.py` lines 153-188 (else branch)
- Falls through to `create_or_update_message()` with `update_only=True`
- Collapsed template rendered (line 132-134)

### 3. Draft → Ready (Undraft)

**Actions**: `update` with `changes.draft: {previous: true, current: false}`

**Behavior**:
- **DELETE old draft messages immediately** (deletion_delay=0)
- **CREATE new messages** to bring MR back to attention
- Purpose: Reset notification state, make MR visible again

**Code Path**: `webhook/merge_request.py` lines 82-104
- Detects draft→ready transition via `mr.changes` inspection
- Calls `update_all_messages_transactional()` with `schedule_deletion=True, deletion_delay=0`
- Then falls through to create new messages

**Why Immediate Deletion**:
- Old draft messages should disappear quickly
- New messages replace them to re-notify channel members

### 4. MR Close / Merge / Delete

**Actions**: `merge`, `close`, or `state` in `["closed", "merged"]`

**Behavior**:
- **UPDATE all existing messages** with final state
- **Schedule for deletion** after configured delay (`MESSAGE_DELETE_DELAY_SECONDS`)
- No new messages created
- Collapsed template with final status icon

**Code Path**: `webhook/merge_request.py` lines 142-152
- Calls `update_all_messages_transactional()` with configured delay
- Periodic cleanup job (`periodic_cleanup`) deletes messages after delay expires

**Purpose**:
- Show final MR state briefly
- Clean up channel after delay (avoid clutter)

## Multi-Channel Processing

### Example Scenario 1: Multiple Webhooks at MR Creation

GitLab project has 3 webhooks configured:
- Webhook 1: Team Channel A (`conversation_token_1`) - filters for team members
- Webhook 2: Team Channel B (`conversation_token_2`) - filters for reviewers only
- Webhook 3: Manager Channel (`conversation_token_3`) - no filtering

When an MR is opened:
1. GitLab sends 3 separate webhook calls (same payload, different targets)
2. Each webhook processes independently:
   - Call 1 → Creates message in Channel A (if participant matches)
   - Call 2 → Creates message in Channel B (if user is reviewer)
   - Call 3 → Creates message in Manager Channel (always)
3. Result: Up to 3 messages created (one per channel) for the same MR

**Critical Design Point**: The system **must not** deduplicate identical payloads across different conversation tokens, as each represents a distinct channel/audience.

### Example Scenario 2: Late Webhook Registration (Dynamic Channel Addition)

**Initial State:**
```
MR #123 opened
- Webhook A configured → Message created in Channel A
- Webhook B configured → Message created in Channel B
Database: 2 message refs (A, B)
```

**Admin adds Webhook C:**
```
Time 1: Admin configures new Webhook C for Channel C
Time 2: MR #123 updated (assignee changed)
        GitLab sends: Webhook A + Webhook B + Webhook C
```

**What happens to Webhook C?**

1. **Webhook C processes MR #123 update**
   - Calls `get_or_create_message_refs(mr_ref_id, [conv_token_C])`
   - Queries database: Finds existing refs for A, B (but NOT C)
   - Creates new message ref for conv_token_C with `message_id=NULL`

2. **Message creation logic** (`create_or_update_message`)
   - Detects `message_id=NULL` (no existing message)
   - Checks `update_only` flag:
     ```python
     update_only = (
         (action not in ("open", "reopen") and is_draft)
         or not participant_found
     )
     ```
   - For `action="update"`, `draft=False`, `participant_found=True`:
     - `update_only = False`
   - **Creates new message** via POST to Teams API
   - Stores message_id in database

3. **Result**: ✅ **Message successfully created in Channel C**

**Key Insight**: The system gracefully handles late webhook registration. When a new webhook is added to an existing MR, the first relevant update will:
- Create the missing message ref
- Post the message to the new channel
- Track it alongside existing messages

**When would the message NOT be created?**

The system will skip message creation if:

| Condition                        | Why?                                  | Example                                                         |
| -------------------------------- | ------------------------------------- | --------------------------------------------------------------- |
| MR is in draft state             | Draft updates shouldn't spam channels | `update_only=True` if draft                                     |
| Participant filter doesn't match | User not relevant to this channel     | Webhook C filters for "managers" but MR has no manager assigned |
| Action is merge/close            | Terminal states use batch update path | Would update ALL existing messages, not create new ones         |

**Code Flow**:
- `webhook/merge_request.py` lines 154-177: Handles per-channel operations
- `webhook/messaging.py` lines 23-78: `get_or_create_message_refs()` creates missing refs
- `webhook/messaging.py` lines 99-122: `create_or_update_message()` creates message if `message_id=NULL` and `update_only=False`

## Filtering Behavior

### Participant ID Filter

The `participant_ids_filter` parameter determines message creation eligibility:

```python
participant_found = (
    opener.id in participant_ids_filter OR
    any assignee.id in participant_ids_filter OR
    any reviewer.id in participant_ids_filter
)
```

**If `participant_found=False`**:
- Existing messages are updated (`update_only=True`)
- New messages are NOT created
- Purpose: Update existing visibility without spamming irrelevant users

**Code**: `webhook/merge_request.py` lines 41-50, 170-176

## Fingerprint Usage and Deduplication Strategy

### Per-Message-Ref Fingerprinting

The system uses **per-message-ref fingerprints** to ensure efficient message updates while preventing unnecessary Teams API calls.

**Design**:
- `last_processed_fingerprint` column on `merge_request_message_ref` table
- Fingerprint = SHA256 hash of webhook payload JSON
- ALL webhook operations (open, update, approve, etc.) process ALL message refs
- Per-ref decision: skip if fingerprint matches, update if different, create if NULL

**Key Benefits**:
- ✅ **Solves orphaned message staleness**: Removed webhook's messages stay up-to-date
- ✅ **Reduces redundant API calls**: Skip Teams PATCH if content unchanged
- ✅ **Multi-channel safe**: Each webhook can process all refs independently
- ✅ **Graceful late registration**: New webhooks create messages on first relevant update

### Operation Types and Fingerprint Behavior

The system uses fingerprints at the per-message-ref level:

#### 1. Non-Closing Operations (open, reopen, update, approve, unapprove)

**Actions**: `open`, `reopen`, `update`, `approved`, `unapproved`

**Flow** (`webhook/merge_request.py` lines 155-210):
```python
# Ensure message refs exist for webhook's conversation tokens
await get_or_create_message_refs(mr_ref_id, conversation_tokens)

# Get ALL message refs (including orphaned ones from removed webhooks)
all_message_refs = await get_all_message_refs(mr_ref_id)

for mrmsgref in all_message_refs:
    # Skip if already processed with this fingerprint
    if mrmsgref.last_processed_fingerprint == payload_fingerprint:
        continue

    # Create message if ref has no message_id (and in webhook's tokens)
    if mrmsgref.message_id is None:
        if conv_token in conversation_tokens:
            create_or_update_message() + store fingerprint
    # Update message if fingerprint differs
    else:
        update_message_with_fingerprint()
```

**Example - Removed Webhook (Orphaned Message)**:
```
Initial state:
- MR #123 has 3 messages (Channels A, B, C)
- Admin removes Webhook B from GitLab

MR #123 updated (assignee changed):
- GitLab sends: Webhook A + Webhook C (NOT B - removed)
- Webhook A arrives:
  - Gets ALL 3 message refs (including orphaned B)
  - For ref A: fingerprint differs → update message A
  - For ref B: fingerprint differs → update message B (orphaned but updated!)
  - For ref C: fingerprint differs → update message C
- Webhook C arrives:
  - Gets ALL 3 message refs
  - For ref A: fingerprint matches → skip
  - For ref B: fingerprint matches → skip
  - For ref C: fingerprint matches → skip (already updated by Webhook A)

Result: All 3 messages updated, including orphaned Channel B ✓
```

**This solves the orphaned message problem**: Even though Webhook B is removed, its message stays up-to-date via remaining webhooks.

#### 2. Closing Operations (merge, close, draft→ready)

**Actions**:
- Draft→Ready transition (`update` with `changes.draft: {previous: true, current: false}`)
- Close/Merge (`merge`, `close`, `state` in `["closed", "merged"]`)

**Flow** (`webhook/merge_request.py` lines 144-154):
```python
# Use transactional batch update (locks ALL message refs)
await update_all_messages_transactional(
    mri, card, summary, payload_fingerprint,
    "close/merge",
    schedule_deletion=True,
    deletion_delay=...
)
```

**Behavior**:
- Locks ALL message refs with `FOR UPDATE`
- Updates all messages via Teams API
- Schedules all for deletion
- First webhook to acquire lock wins, others find refs already deleted

**Code**: `update_all_messages_transactional()` in `webhook/messaging.py` lines 266-377:
```python
async with connection.transaction():
    locked_messages = await connection.fetch(
        """SELECT * FROM merge_request_message_ref
           WHERE merge_request_ref_id = $1
           FOR UPDATE""",
        mr_ref_id,
    )
    # Update all messages via Teams API
    # Schedule all for deletion
    # Store fingerprint
    # Delete all message refs
```

**Example - Merge with Removed Webhook**:
```
Initial state:
- MR #123 has 3 messages (Channels A, B, C)
- Admin removes Webhook B from GitLab

MR #123 merged:
- GitLab sends: Webhook A + Webhook C (NOT B - removed)
- Webhook A arrives first:
  - Locks ALL 3 message refs (including orphaned B)
  - Updates all 3 messages via Teams API
  - Schedules all 3 for deletion
  - Stores fingerprint_X
  - Deletes all 3 message refs from DB
- Webhook C arrives:
  - Tries to lock message refs → none found (already deleted)
  - No-op, no errors

Result: All messages (including orphaned Channel B) updated/deleted exactly once ✓
```

### Implementation Details

**Per-Message-Ref Fingerprint Storage**:
- Stored in `merge_request_message_ref.last_processed_fingerprint` column
- Updated atomically with message updates via `update_message_with_fingerprint()`
- NULL fingerprints = never processed (new message ref or pre-migration)
- Matching fingerprints = skip (already processed with this payload)
- Different fingerprints = update (content changed)

**Key Functions**:
1. `get_all_message_refs(mr_ref_id)` - Retrieves ALL refs regardless of conversation tokens
2. `update_message_with_fingerprint(client, mrmsgref, card, summary, fingerprint)` - Updates message + stores fingerprint
3. `update_all_messages_transactional(...)` - Batch update with per-ref fingerprint checking (skips refs with matching fingerprint for non-deletion ops)

**No Cleanup Required**: Per-message-ref fingerprints persist with their message refs and are deleted when message refs are deleted (no unbounded growth).

### Summary Table

| Action                 | Scope            | Message Refs Processed | Per-Ref Fingerprint Check? | Per-Ref Fingerprint Store?  | Deduplication Strategy          |
| ---------------------- | ---------------- | ---------------------- | -------------------------- | --------------------------- | ------------------------------- |
| `open`                 | All refs         | **ALL**                | ✅ YES (skip if match)     | ✅ YES (on update/create)   | Per-ref (allows multi-webhook)  |
| `reopen`               | All refs         | **ALL**                | ✅ YES (skip if match)     | ✅ YES (on update/create)   | Per-ref (allows multi-webhook)  |
| `update` (normal)      | All refs         | **ALL**                | ✅ YES (skip if match)     | ✅ YES (on update/create)   | Per-ref (allows multi-webhook)  |
| `update` (draft→ready) | **Global batch** | **ALL**                | ❌ NO (always process)     | ❌ NO (refs deleted)        | Row lock (refs deleted after)   |
| `approved`             | All refs         | **ALL**                | ✅ YES (skip if match)     | ✅ YES (on update/create)   | Per-ref (allows multi-webhook)  |
| `unapproved`           | All refs         | **ALL**                | ✅ YES (skip if match)     | ✅ YES (on update/create)   | Per-ref (allows multi-webhook)  |
| `merge`                | **Global batch** | **ALL**                | ❌ NO (always process)     | ❌ NO (refs deleted)        | Row lock (refs deleted after)   |
| `close`                | **Global batch** | **ALL**                | ❌ NO (always process)     | ❌ NO (refs deleted)        | Row lock (refs deleted after)   |
| `emoji`                | All refs         | **ALL**                | ✅ YES (skip if match)     | ✅ YES (on update)          | Per-ref (allows multi-webhook)  |
| `pipeline`             | All refs         | **ALL**                | ✅ YES (skip if match)     | ✅ YES (on update)          | Per-ref (allows multi-webhook)  |

**Key Principles**:
- ALL operations process ALL message refs (including orphaned ones)
- Per-ref fingerprints enable efficient multi-webhook processing (skip redundant updates)
- Global batch operations use transactional locks for exactly-once semantics

## Race Condition Protections

### Database Constraints

**1. Unique GitLab Instance** (`gitlab_instance_hostname_lower_uniq`)
- Ensures one database record per GitLab hostname
- Concurrent webhooks from same instance reuse existing record

**2. Unique MR Reference** (`merge_request_ref_mr_identity_uniq`)
- Key: `(gitlab_instance_id, gitlab_project_id, gitlab_merge_request_iid)`
- Ensures one MR ref per GitLab merge request
- Concurrent webhooks for same MR reuse existing record

**3. Unique Message Reference** (`mr_ref_conv_token_uniq`)
- Key: `(merge_request_ref_id, conversation_token)`
- Ensures one message per MR per Teams channel
- Prevents duplicate messages in same channel

### Row Locking

**`FOR UPDATE` locks** prevent concurrent data corruption:

**1. In `get_or_create_message_refs()`** (`webhook/messaging.py` line 32-36)
```sql
SELECT 1 FROM merge_request_ref
WHERE merge_request_ref_id = $1
FOR UPDATE
```
- Serializes concurrent access to message references
- Ensures atomic create-or-get operations

**2. In `update_all_messages_transactional()`** (`webhook/messaging.py` line 217-270)
```sql
SELECT * FROM merge_request_message_ref
WHERE merge_request_ref_id = $1
FOR UPDATE
```
- Locks all message refs before batch updates
- Prevents lost updates during concurrent modifications

### Transactional Updates

All database modifications occur within transactions to ensure:
- Atomicity: All changes succeed or all fail
- Consistency: Database constraints always enforced
- Isolation: Concurrent operations don't see partial state

## Code Reference Map

| Functionality          | File                       | Lines   | Function                              |
| ---------------------- | -------------------------- | ------- | ------------------------------------- |
| Main webhook handler   | `webhook/merge_request.py` | 22-195  | `merge_request()`                     |
| Message ref management | `webhook/messaging.py`     | 23-73   | `get_or_create_message_refs()`        |
| Create/update messages | `webhook/messaging.py`     | 76-180  | `create_or_update_message()`          |
| Batch message updates  | `webhook/messaging.py`     | 183-272 | `update_all_messages_transactional()` |
| Card rendering         | `cards/render.py`          | -       | `render()`                            |
| Database helpers       | `db.py`                    | 122-278 | `DBHelper` class                      |

## Testing

Comprehensive E2E race condition tests validate these behaviors:

- `tests/test_e2e_race_conditions.py::test_e2e_race_duplicate_webhook_processing`
  - Validates: 10 identical webhooks only create 1 set of records
  - Protection: Database unique constraints

- `tests/test_e2e_webhook_flow.py`
  - Validates: Full webhook flow with real database
  - Scenarios: Open, merge, draft transitions, multiple channels

See `tests/` directory for complete test suite.

## Configuration

### Environment Variables

- `MESSAGE_DELETE_DELAY_SECONDS`: Delay before deleting close/merge messages (default: 300)
- `DATABASE_URL`: PostgreSQL connection string
- `DATABASE_POOL_MIN_SIZE` / `DATABASE_POOL_MAX_SIZE`: Connection pool sizing

### Webhook Setup

Each GitLab webhook should be configured with:
- URL: `https://your-domain/webhooks/merge_request`
- Trigger: Merge request events
- Custom headers: Authentication tokens if required
- POST payload: All merge request event types

## Summary

The system is designed for **multi-channel, filtered notification delivery**:
- One MR event → Multiple webhooks → Multiple messages
- Each webhook processes independently with its own conversation token
- Database constraints prevent accidental duplicates
- Row locking prevents concurrent corruption
- Message lifecycle follows GitLab MR state transitions
- Filtering ensures relevant users are notified

### Dynamic Webhook Management

The system elegantly handles webhook configuration changes:

**Adding webhooks** (late registration):
- New webhook added to existing MR
- Next update: `get_or_create_message_refs()` creates missing message ref
- Message posted to new channel automatically
- ✅ No messages lost

**Removing webhooks** (orphaned messages):
- Webhook removed but messages still exist in Teams
- Next terminal action (merge/close): Remaining webhooks update ALL messages
- `FOR UPDATE` lock on `merge_request_ref_id` includes orphaned messages
- ✅ Orphaned messages still updated/deleted

**Key principle**: Message refs are tied to MR, not to active webhooks. This ensures consistency even as webhook configuration changes.
