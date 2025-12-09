#!/usr/bin/env python3
"""Tests for cards/render.py - YAML template rendering."""
from cards.render import render
from cards.render import yaml_escape_sq
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
            path_with_namespace="test/project",
            web_url="https://gitlab.example.com/test/project",
        ),
        object_attributes=GLMRAttributes(
            id=123,
            iid=1,
            title=title,
            created_at="2025-01-01T00:00:00Z",
            draft=False,
            state="opened",
            url="https://gitlab.example.com/test/project/-/merge_requests/1",
            action="open",
            updated_at="2025-01-01T00:00:00Z",
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
