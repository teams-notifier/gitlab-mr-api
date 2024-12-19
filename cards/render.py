#!/usr/bin/env python3
import datetime
import json
from enum import Enum
from typing import Any

import yaml
from jinja2 import Environment
from jinja2 import FileSystemLoader
from jinja2 import Template
from pydantic import BaseModel

from db import MergeRequestInfos


class Teams_Color(Enum):
    DEFAULT = "default"
    DARK = "dark"
    LIGHT = "light"
    ACCENT = "accent"
    GOOD = "good"
    WARNING = "warning"
    ATTENTION = "attention"


class EmojiCount(BaseModel):
    name: str
    count: int


def render(mri: MergeRequestInfos) -> dict[str, Any]:
    env = Environment(loader=FileSystemLoader("./cards/"))
    templ: Template = env.get_template("merge_request.yaml.j2")

    fallback = (
        f"MR {mri.merge_request_payload.object_attributes.state}:"
        f" {mri.merge_request_payload.object_attributes.title}"
        f" on {mri.merge_request_payload.project.path_with_namespace}"
    )

    latest_pipeline_obj = mri.merge_request_extra_state.pipeline_statuses.get(str(mri.head_pipeline_id))
    latest_pipeline_infos = {}
    if latest_pipeline_obj:
        latest_pipeline_infos["status"] = latest_pipeline_obj.object_attributes.detailed_status
        latest_pipeline_infos["color"], latest_pipeline_infos["weight"] = {
            "failed": ["attention", "Bolder"],
            "passed": ["good", "Bolder"],
        }.get(latest_pipeline_obj.object_attributes.detailed_status, ["default", "default"])

    approvers = [
        approver.name
        for approver in mri.merge_request_extra_state.approvers.values()
        if approver.status == "approved"
    ]

    assignees = [assignees.name for assignees in mri.merge_request_payload.assignees]

    reviewers = [reviewers.name for reviewers in mri.merge_request_payload.reviewers]

    icon_color: Teams_Color = Teams_Color.ACCENT

    emoji_count: dict[str, int] = {}
    emoji_keys = list(mri.merge_request_extra_state.emojis.keys())
    emoji_keys.sort()
    for key in emoji_keys:
        emoji_name, _ = key.split(":")
        if mri.merge_request_extra_state.emojis[key].event_type == "award":
            if emoji_name not in emoji_count:
                emoji_count.setdefault(emoji_name, 0)
            emoji_count[emoji_name] = emoji_count[emoji_name] + 1

    emojis: list[EmojiCount] = [EmojiCount(name=name, count=count) for name, count in emoji_count.items()]
    emojis.sort(key=lambda x: (-x.count, x.name))

    if mri.merge_request_payload.object_attributes.action == "close":
        icon_color = Teams_Color.ATTENTION
    if mri.merge_request_payload.object_attributes.action == "merge":
        icon_color = Teams_Color.GOOD

    precalc = {
        "path_with_namespace": mri.merge_request_payload.project.path_with_namespace,
        "iid": mri.merge_request_payload.object_attributes.iid,
        "title": mri.merge_request_payload.object_attributes.title,
        "opener": mri.merge_request_extra_state.opener,
        "detailed_merge_status": mri.merge_request_payload.object_attributes.detailed_merge_status,
        "latest_action": mri.merge_request_payload.object_attributes.action,
        "source_branch": mri.merge_request_payload.object_attributes.source_branch,
        "target_branch": mri.merge_request_payload.object_attributes.target_branch,
        "project_url": mri.merge_request_payload.project.web_url,
        "url": mri.merge_request_payload.object_attributes.url,
        "latest_pipeline": latest_pipeline_infos,
        "approvers": approvers,
        "assignees": assignees,
        "reviewers": reviewers,
        "icon_color": icon_color.value,
        "emojis": emojis,
    }

    rendered = templ.render(
        precalc=precalc,
        mri=mri,
        fallback=json.dumps(fallback),
        now=datetime.datetime.now(),
    )
    # print(rendered)
    # open("/tmp/notiteams-gitlab-mr-api-OUTPUT.yaml", "w").write(rendered)
    result_as_json: dict[str, Any] = yaml.safe_load(rendered)

    return result_as_json
