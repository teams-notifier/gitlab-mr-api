#!/usr/bin/env python3
from __future__ import annotations

import re

from typing import TYPE_CHECKING
from typing import Any
from urllib.parse import quote

import fastapi_structured_logging
import httpx

from config import config


if TYPE_CHECKING:
    from db import MergeRequestExtraState

logger = fastapi_structured_logging.get_logger()


def _get_next_page_url(link_header: str) -> str | None:
    """Parse GitLab Link header and return the 'next' page URL if present."""
    if not link_header:
        return None
    for part in link_header.split(","):
        match = re.match(r'<([^>]+)>;\s*rel="next"', part.strip())
        if match:
            return match.group(1)
    return None


async def fetch_and_persist_discussion_stats(
    merge_request_ref_id: int,
    project_url: str,
    project_id: int,
    mr_iid: int,
) -> MergeRequestExtraState | None:
    """
    Fetch MR discussion stats from GitLab API and persist to database.
    Returns updated MergeRequestExtraState or None if fetch failed.
    """
    from db import DiscussionStats as DBDiscussionStats
    from db import dbh

    stats = await _fetch_mr_discussion_stats(project_url, project_id, mr_iid)
    if stats is None:
        return None

    db_stats = DBDiscussionStats(
        threads_total=stats["threads_total"],
        threads_resolved=stats["threads_resolved"],
        threads_unresolved=stats["threads_unresolved"],
        comments_total=stats["comments_total"],
        comments_resolved=stats["comments_resolved"],
        comments_unresolved=stats["comments_unresolved"],
    )
    return await dbh.update_discussion_stats(merge_request_ref_id, db_stats)


async def _fetch_mr_discussion_stats(
    project_url: str,
    project_id: int,
    mr_iid: int,
) -> dict[str, int] | None:
    """
    Opportunistically fetch MR discussions and count resolved/unresolved threads and comments.
    Ignores system notes. Returns None if API token not configured or any error occurs.
    """
    api_token = config.get_gitlab_api_token(project_url)
    if api_token is None:
        logger.debug("no gitlab api token configured for project", project_url=project_url)
        return None

    encoded_project_id = quote(str(project_id), safe="")
    base_url = (
        f"{api_token.url.rstrip('/')}/api/v4/projects/{encoded_project_id}"
        f"/merge_requests/{mr_iid}/discussions"
    )

    try:
        timeout = httpx.Timeout(5.0, connect=2.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            discussions: list[dict[str, Any]] = []
            url: str | None = base_url
            params: dict[str, int] | None = {"per_page": 100}

            while url:
                response = await client.get(
                    url,
                    headers={"PRIVATE-TOKEN": api_token.token},
                    params=params,
                )
                response.raise_for_status()
                discussions.extend(response.json())

                url = _get_next_page_url(response.headers.get("link", ""))
                params = None

            threads_total = 0
            threads_resolved = 0
            threads_unresolved = 0
            comments_total = 0
            comments_resolved = 0
            comments_unresolved = 0

            for discussion in discussions:
                notes = discussion.get("notes", [])
                user_notes = [n for n in notes if not n.get("system", False)]

                if not user_notes:
                    continue

                if discussion.get("individual_note", False):
                    for note in user_notes:
                        if note.get("resolvable", False):
                            comments_total += 1
                            if note.get("resolved", False):
                                comments_resolved += 1
                            else:
                                comments_unresolved += 1
                else:
                    resolvable_notes = [n for n in user_notes if n.get("resolvable", False)]
                    if not resolvable_notes:
                        continue

                    threads_total += 1
                    all_resolved = all(n.get("resolved", False) for n in resolvable_notes)
                    if all_resolved:
                        threads_resolved += 1
                    else:
                        threads_unresolved += 1

                    for note in resolvable_notes:
                        comments_total += 1
                        if note.get("resolved", False):
                            comments_resolved += 1
                        else:
                            comments_unresolved += 1

            logger.info(
                "fetched mr discussion stats",
                project_id=project_id,
                mr_iid=mr_iid,
                threads_total=threads_total,
                threads_resolved=threads_resolved,
                threads_unresolved=threads_unresolved,
                comments_total=comments_total,
                comments_resolved=comments_resolved,
                comments_unresolved=comments_unresolved,
            )
            return {
                "threads_total": threads_total,
                "threads_resolved": threads_resolved,
                "threads_unresolved": threads_unresolved,
                "comments_total": comments_total,
                "comments_resolved": comments_resolved,
                "comments_unresolved": comments_unresolved,
            }

    except httpx.HTTPStatusError as e:
        logger.warning(
            "gitlab api http error",
            token_name=api_token.name,
            api_url=base_url,
            project_id=project_id,
            mr_iid=mr_iid,
            status_code=e.response.status_code,
        )
        return None
    except Exception as e:
        logger.warning(
            "gitlab api error",
            token_name=api_token.name,
            api_url=base_url,
            project_id=project_id,
            mr_iid=mr_iid,
            error=str(e),
        )
        return None
