# 001 — Repost the card once on draft-to-ready

**Status**: shipped
**Requested by**: Andy Trouvé
**Date**: 2026-11-06

## Problem

Observed live when an MR leaves draft:

- MR opened as draft: card posted in Teams (OK).
- MR marked ready: card reposted without the draft marker, then deleted right after.
- Next action on the MR (e.g. approve): card reappears in the channel.

Cause: GitLab fans one MR event out to 3-4 hooks (3 group hooks, sometimes a project hook), all with
the same payload carrying `changes.draft`. Since #48 the per-MR lock runs them one after another, and
each replays the draft-to-ready step. Delivery 2 schedules immediate deletion of the card delivery 1
just posted and drops its ref. The next event finds no ref and creates the card again.

## Solution

The draft-to-ready repost runs once per GitLab event, whatever the number of hooks delivering it.
The old draft card is deleted, one fresh card is posted per channel, and it stays there.

## Scope

- Claim each draft-to-ready event once: a monotonic compare-and-set on a `draft_to_ready_at` key in
  `merge_request_ref.merge_request_extra_state` (value = the event's `updated_at`), written only by
  this step and moved forward only (newer `updated_at` wins). The delivery that wins the claim runs
  the delete-and-repost; the others skip it.
- Later deliveries still run the normal path: create the card in their own channels if missing.
- Update `docs/workflow.md` section 3 (Draft → Ready) to state the step runs once per event.

## Out of scope

- Same idempotence audit for the threads-resolved repost: it keys on DB state
  (`had_unresolved_threads`), already persisted by delivery 1, so later deliveries skip it.
- Reducing the number of GitLab hooks per project.
- Changing the lock behaviour of #48 (including the 30s unlocked fallback).

## Acceptance criteria

- [x] **AC1** Given a draft MR with a card in channel X, when the draft-to-ready event is delivered
  by several hooks, then the old draft card is deleted and exactly one new card remains in X.
- [x] **AC2** Given a draft-to-ready event whose repost already ran for an earlier delivery, when
  another delivery of it arrives, then no message is scheduled for deletion and no message ref is
  deleted.
- [x] **AC3** Given that replayed delivery targets a channel with no card yet, then the card is
  created in that channel.
- [x] **AC4** Given the first delivery of a draft-to-ready event, then the old draft cards are
  scheduled for immediate deletion and new cards are posted (behaviour unchanged).
- [x] **AC5** Given the message refs were already stamped with the event's `updated_at` by another
  writer (e.g. the emoji-triggered GitLab API refresh), when the first delivery of the draft-to-ready
  event arrives, then the repost still runs.
- [x] **AC6** Given an MR that went ready, back to draft, then ready again (new `updated_at`), when
  the second ready event arrives, then the repost runs again.
- [x] **AC7** Given a draft-to-ready event already claimed, when a late delivery of an older
  draft-to-ready event of the same MR arrives, then the repost does not run.

## Phases

### Phase 1 — Claim the repost once per event
- Work: compare-and-set of `draft_to_ready_at` guarding the draft-to-ready branch in
  `webhook/merge_request.py`, unit + e2e tests, `docs/workflow.md`.
- **Covers**: AC1, AC2, AC3, AC4, AC5, AC6, AC7
- **Data model impact**: new jsonb key `draft_to_ready_at` in `merge_request_extra_state`; no migration
- **DoD**: `pytest` green, including a test per AC (AC1 as delivery 1 then delivery 2 of the same
  event); the AC2 test fails with the guard removed; `pre-commit run --all-files` passes.

## Data model impact (summary)

- `merge_request_ref.merge_request_extra_state`: new key `draft_to_ready_at` (ISO timestamp string).
  No migration: absent key = never claimed. Every SQL writer of this column uses `jsonb_set`, so the
  key survives; the Pydantic model ignores unknown keys, so it stays out of the card fingerprint.

## Open questions

None.

## Decisions

- Dedup on a dedicated marker written only by the repost, not on the stored payload's previous
  `draft` value nor on `last_processed_updated_at` — because `refresh_mr_payload_from_api` rewrites
  the stored payload and stamps the refs with GitLab's `updated_at`, which would make the first
  delivery look like a replay (review round 1, H2, user decision 2026-11-06).
- Compare-and-set, not read-then-write — so the repost stays single even on the 30s lock-timeout
  path that runs unlocked (review L1), without touching the lock.
- Monotonic: claim only when the stored `draft_to_ready_at` is absent or older, compared as
  `timestamptz` (offset-independent) — so a late older event never re-claims and a later replay of
  the newer one never reposts twice (review round 2).
- Accepted limitation: GitLab's `updated_at` has 1s resolution; ready→draft→ready within one second
  gives two ready events with the same `updated_at`, the second is taken for a replay and the card is
  patched in place instead of reposted (review round 3, L2).
- Accepted limitation: a non-timestamp `draft_to_ready_at` value makes the claim raise. Only
  `_claim_draft_to_ready()` writes the key, always from an offset-aware `isoformat()`, so only a
  manual DB edit can reach it (review round 3, L1).
- Claim before the repost, not after — a repost that fails after the claim leaves the old cards in
  place, patched by the normal path: no card is lost, only the "bring back to attention" effect.
- A first prototype keyed on `updated_at` was replaced by this design after review round 1.
