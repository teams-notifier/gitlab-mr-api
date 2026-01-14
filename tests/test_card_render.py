#!/usr/bin/env python3
"""Tests for cards/render.py - YAML template rendering."""

from typing import Any

from cards.render import render
from cards.render import yaml_escape_sq
from db import DiscussionStats
from db import GitlabUser
from db import MergeRequestExtraState
from db import MergeRequestInfos
from gitlab_model import GLMRAttributes
from gitlab_model import GLMRRepo
from gitlab_model import GLProject
from gitlab_model import GLUser
from gitlab_model import MergeRequestPayload


def make_mri(
    title: str = "Test MR",
    opener_name: str = "Test User",
    source_branch: str = "feature",
    target_branch: str = "main",
    assignees: list[GLUser] | None = None,
    reviewers: list[GLUser] | None = None,
    approvers: list[str] | None = None,
    action: str = "open",
    draft: bool = False,
    discussion_stats: DiscussionStats | None = None,
) -> MergeRequestInfos:
    """Create a MergeRequestInfos for testing."""
    payload = MergeRequestPayload(
        object_kind="merge_request",
        event_type="merge_request",
        repository=GLMRRepo(
            homepage="https://gitlab.example.com/test/project",
            name="project",
        ),
        user=GLUser(
            id=1,
            username="testuser",
            name=opener_name,
            email="test@example.com",
        ),
        project=GLProject(
            id=1,
            path_with_namespace="test/project",
            web_url="https://gitlab.example.com/test/project",
        ),
        object_attributes=GLMRAttributes(
            id=123,
            iid=1,
            title=title,
            created_at="2025-01-01 00:00:00 UTC",
            draft=draft,
            state="opened",
            url="https://gitlab.example.com/test/project/-/merge_requests/1",
            action=action,
            updated_at="2025-01-01 00:00:00 UTC",
            detailed_merge_status="mergeable",
            head_pipeline_id=None,
            work_in_progress=False,
            source_project_id=100,
            source_branch=source_branch,
            target_project_id=100,
            target_branch=target_branch,
        ),
        changes={},
        assignees=assignees or [],
        reviewers=reviewers or [],
    )

    extra_state = MergeRequestExtraState(
        version=1,
        opener=GitlabUser(id=1, username="testuser", name=opener_name),
        approvers={},
        pipeline_statuses={},
        emojis={},
        discussion_stats=discussion_stats,
    )

    if approvers:
        from db import GitlabApprovals

        for i, name in enumerate(approvers):
            extra_state.approvers[str(i)] = GitlabApprovals(
                id=i, username=f"user{i}", name=name, status="approved"
            )

    return MergeRequestInfos(
        merge_request_ref_id=1,
        merge_request_payload=payload,
        merge_request_extra_state=extra_state,
        head_pipeline_id=None,
    )


class TestYamlEscapeSq:
    """Tests for the yaml_escape_sq filter."""

    def test_no_quotes(self):
        assert yaml_escape_sq("hello world") == "hello world"

    def test_single_quote(self):
        assert yaml_escape_sq("it's working") == "it''s working"

    def test_multiple_quotes(self):
        assert yaml_escape_sq("don't won't can't") == "don''t won''t can''t"

    def test_consecutive_quotes(self):
        assert yaml_escape_sq("test''value") == "test''''value"

    def test_quote_at_start(self):
        assert yaml_escape_sq("'hello") == "''hello"

    def test_quote_at_end(self):
        assert yaml_escape_sq("hello'") == "hello''"

    def test_only_quotes(self):
        assert yaml_escape_sq("'''") == "''''''"

    def test_empty_string(self):
        assert yaml_escape_sq("") == ""


class TestRenderWithQuotes:
    """Tests for render() with single quotes in user content."""

    def test_title_with_single_quote(self):
        mri = make_mri(title="Fix: it's broken")
        result = render(mri)
        assert result["type"] == "AdaptiveCard"
        assert "Fix: it's broken" in str(result)

    def test_opener_name_with_quote(self):
        mri = make_mri(opener_name="O'Brien")
        result = render(mri)
        assert "O'Brien" in str(result)

    def test_branch_with_quote(self):
        mri = make_mri(source_branch="feature/john's-fix")
        result = render(mri)
        assert "feature/john's-fix" in str(result)

    def test_assignee_with_quote(self):
        assignee = GLUser(id=2, username="obrien", name="O'Malley", email="o@example.com")
        mri = make_mri(assignees=[assignee])
        result = render(mri)
        assert "O'Malley" in str(result)

    def test_reviewer_with_quote(self):
        reviewer = GLUser(id=3, username="reviewer", name="Jean-Pierre D'Arc", email="jp@example.com")
        mri = make_mri(reviewers=[reviewer])
        result = render(mri)
        assert "Jean-Pierre D'Arc" in str(result)

    def test_approver_with_quote(self):
        mri = make_mri(approvers=["Patrick O'Neil"])
        result = render(mri)
        assert "Patrick O'Neil" in str(result)

    def test_multiple_quotes_in_title(self):
        mri = make_mri(title="chore: update 'foo' and 'bar' configs")
        result = render(mri)
        assert "chore: update 'foo' and 'bar' configs" in str(result)


class TestRenderOutput:
    """Tests for valid render() output structure."""

    def test_returns_dict(self):
        mri = make_mri()
        result = render(mri)
        assert isinstance(result, dict)

    def test_adaptive_card_structure(self):
        mri = make_mri()
        result = render(mri)
        assert result["type"] == "AdaptiveCard"
        assert result["version"] == "1.5"
        assert "body" in result

    def test_collapsed_mode(self):
        mri = make_mri()
        result = render(mri, collapsed=True, show_collapsible=True)
        assert isinstance(result, dict)

    def test_fallback_text_present(self):
        mri = make_mri(title="Test Title")
        result = render(mri)
        assert "fallbackText" in result
        assert "Test Title" in result["fallbackText"]


class TestComputeMriFingerprint:
    """Tests for compute_mri_fingerprint()."""

    def test_fingerprint_is_stable(self):
        """Same inputs should produce same fingerprint."""
        from db import compute_mri_fingerprint

        mri = make_mri(title="Test MR")
        fp1 = compute_mri_fingerprint(mri)
        fp2 = compute_mri_fingerprint(mri)
        assert fp1 == fp2

    def test_fingerprint_changes_with_input(self):
        """Different inputs should produce different fingerprints."""
        from db import compute_mri_fingerprint

        mri1 = make_mri(title="Test MR 1")
        mri2 = make_mri(title="Test MR 2")
        fp1 = compute_mri_fingerprint(mri1)
        fp2 = compute_mri_fingerprint(mri2)
        assert fp1 != fp2

    def test_fingerprint_is_sha256(self):
        """Fingerprint should be a valid SHA256 hex string."""
        from db import compute_mri_fingerprint

        mri = make_mri()
        fp = compute_mri_fingerprint(mri)
        assert len(fp) == 64
        assert all(c in "0123456789abcdef" for c in fp)


def find_icon_color(card: dict[str, Any]) -> str | None:
    """Find the icon color in the adaptive card body."""
    for item in card.get("body", []):
        if item.get("type") == "ColumnSet":
            for col in item.get("columns", []):
                for inner in col.get("items", []):
                    if inner.get("type") == "Icon":
                        color = inner.get("color")
                        return str(color) if color is not None else None
    return None


def find_icon_name(card: dict[str, Any]) -> str | None:
    """Find the icon name in the adaptive card body."""
    for item in card.get("body", []):
        if item.get("type") == "ColumnSet":
            for col in item.get("columns", []):
                for inner in col.get("items", []):
                    if inner.get("type") == "Icon":
                        name = inner.get("name")
                        return str(name) if name is not None else None
    return None


class TestIconColorAndName:
    """Tests for icon color and name based on MR state and unresolved threads."""

    def test_default_icon_no_discussion_stats(self):
        """Icon should be BranchRequest with accent color when no discussion stats."""
        mri = make_mri()
        result = render(mri)
        assert find_icon_color(result) == "accent"
        assert find_icon_name(result) == "BranchRequest"

    def test_default_icon_no_unresolved_threads(self):
        """Icon should be BranchRequest with accent color when all threads resolved."""
        stats = DiscussionStats(threads_total=3, threads_resolved=3, threads_unresolved=0)
        mri = make_mri(discussion_stats=stats)
        result = render(mri)
        assert find_icon_color(result) == "accent"
        assert find_icon_name(result) == "BranchRequest"

    def test_chatbubbles_icon_with_unresolved_threads(self):
        """Icon should be CommentError with warning color when unresolved threads."""
        stats = DiscussionStats(threads_total=3, threads_resolved=1, threads_unresolved=2)
        mri = make_mri(discussion_stats=stats)
        result = render(mri)
        assert find_icon_color(result) == "warning"
        assert find_icon_name(result) == "CommentError"

    def test_closed_mr_keeps_attention_regardless_of_threads(self):
        """Closed MR should keep CodeTextOff icon with attention color."""
        stats = DiscussionStats(threads_total=3, threads_resolved=1, threads_unresolved=2)
        mri = make_mri(action="close", discussion_stats=stats)
        result = render(mri)
        assert find_icon_color(result) == "attention"
        assert find_icon_name(result) == "CodeTextOff"

    def test_merged_mr_keeps_good_regardless_of_threads(self):
        """Merged MR should keep Merge icon with good color."""
        stats = DiscussionStats(threads_total=3, threads_resolved=1, threads_unresolved=2)
        mri = make_mri(action="merge", discussion_stats=stats)
        result = render(mri)
        assert find_icon_color(result) == "good"
        assert find_icon_name(result) == "Merge"

    def test_draft_mr_with_unresolved_threads_shows_chatbubbles(self):
        """Draft MR with unresolved threads should show CommentError with warning."""
        stats = DiscussionStats(threads_total=2, threads_resolved=0, threads_unresolved=2)
        mri = make_mri(draft=True, discussion_stats=stats)
        result = render(mri)
        assert find_icon_color(result) == "warning"
        assert find_icon_name(result) == "CommentError"

    def test_draft_mr_without_unresolved_threads_shows_drafts(self):
        """Draft MR without unresolved threads should show Drafts icon with default color."""
        stats = DiscussionStats(threads_total=2, threads_resolved=2, threads_unresolved=0)
        mri = make_mri(draft=True, discussion_stats=stats)
        result = render(mri)
        assert find_icon_color(result) == "default"
        assert find_icon_name(result) == "Drafts"


def find_thread_count_block(card: dict[str, Any]) -> dict[str, Any] | None:
    """Find the threadsCollapsed TextBlock in the card body."""
    for item in card.get("body", []):
        if item.get("type") == "TextBlock" and item.get("id") == "threadsCollapsed":
            return dict(item)
    return None


class TestCollapsedWithThreads:
    """Tests for collapsed state showing thread count."""

    def test_collapsed_with_threads_shows_thread_count(self):
        """Collapsed card with unresolved threads should show 'Threads X/Y resolved' on separate line."""
        stats = DiscussionStats(threads_total=5, threads_resolved=2, threads_unresolved=3)
        mri = make_mri(discussion_stats=stats)
        result = render(mri, collapsed=True, show_collapsible=True)
        thread_block = find_thread_count_block(result)
        assert thread_block is not None
        assert "2/5 resolved" in thread_block.get("text", "")
        assert thread_block.get("color") == "Warning"

    def test_collapsed_without_threads_no_thread_count(self):
        """Collapsed card without unresolved threads should not show thread count block."""
        stats = DiscussionStats(threads_total=3, threads_resolved=3, threads_unresolved=0)
        mri = make_mri(discussion_stats=stats)
        result = render(mri, collapsed=True, show_collapsible=True)
        thread_block = find_thread_count_block(result)
        assert thread_block is None

    def test_collapsed_draft_no_thread_count(self):
        """Collapsed draft without threads should not show thread count block."""
        mri = make_mri(draft=True)
        result = render(mri, collapsed=True, show_collapsible=True)
        thread_block = find_thread_count_block(result)
        assert thread_block is None
