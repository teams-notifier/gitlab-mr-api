#!/usr/bin/env python3
import datetime
import json
from typing import Any

from jinja2 import Environment
from jinja2 import FileSystemLoader
from jinja2 import Template

from db import MergeRequestInfos


def render(mri: MergeRequestInfos) -> dict[str, Any]:
    env = Environment(loader=FileSystemLoader("./cards/"))
    templ: Template = env.get_template("merge_request.json.j2")

    fallback = (
        f"MR {mri.merge_request_payload.object_attributes.state}:"
        f" {mri.merge_request_payload.object_attributes.title}\n"
        f"on {mri.merge_request_payload.project.path_with_namespace}"
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

    precalc = {
        "path_with_namespace": mri.merge_request_payload.project.path_with_namespace,
        "iid": mri.merge_request_payload.object_attributes.iid,
        "title": mri.merge_request_payload.object_attributes.title,
        "openner": mri.merge_request_extra_state.openner,
        "detailed_merge_status": mri.merge_request_payload.object_attributes.detailed_merge_status,
        "latest_action": mri.merge_request_payload.object_attributes.action,
        "source_branch": mri.merge_request_payload.object_attributes.source_branch,
        "target_branch": mri.merge_request_payload.object_attributes.target_branch,
        "url": mri.merge_request_payload.object_attributes.url,
        "latest_pipeline": latest_pipeline_infos,
        "approvers": approvers,
    }

    rendered = templ.render(
        precalc=precalc,
        mri=mri,
        fallback=json.dumps(fallback),
        now=datetime.datetime.now(),
    )
    # print(rendered)
    # open("/tmp/notiteams-gitlab-mr-api-OUTPUT.json", "w").write(rendered)
    result_as_json: dict[str, Any] = json.loads(rendered)

    return result_as_json
