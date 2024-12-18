#!/usr/bin/env python3
from typing import Literal

from pydantic import BaseModel
from pydantic import Field


class GenericPayload(BaseModel, extra="allow"):
    object_kind: Literal["test"]


class GLMRRepo(BaseModel, extra="allow"):
    homepage: str
    name: str


class GLUser(BaseModel, extra="allow"):
    id: int
    name: str
    username: str
    email: str


class GLProject(BaseModel, extra="allow"):
    path_with_namespace: str
    web_url: str


class GLMRAttributes(BaseModel, extra="allow"):
    id: int  # ID is this instance wide merge request id
    iid: int  # IID is this project's merge request id
    title: str
    created_at: str
    draft: bool
    state: str
    url: str
    action: str

    # https://docs.gitlab.com/ee/api/merge_requests.html#merge-status
    detailed_merge_status: str  # mergeable, not_approved

    head_pipeline_id: int | None
    work_in_progress: bool
    source_project_id: int
    source_branch: str
    target_project_id: int
    target_branch: str


class MergeRequestPayload(BaseModel, extra="allow"):
    object_kind: Literal["merge_request"]
    event_type: str
    repository: GLMRRepo
    user: GLUser
    project: GLProject
    object_attributes: GLMRAttributes

    assignees: list[GLUser] = Field(default_factory=list)
    reviewers: list[GLUser] = Field(default_factory=list)


class GLPipelineBuild(BaseModel, extra="allow"):
    id: int
    stage: str
    name: str
    status: str
    failure_reason: str | None


class GLPipelineAttributes(BaseModel, extra="allow"):
    id: int
    detailed_status: str


class PipelinePayload(BaseModel, extra="allow"):
    object_kind: Literal["pipeline"]
    object_attributes: GLPipelineAttributes
    builds: list[GLPipelineBuild]


class GLEmojiAttributes(BaseModel, extra="allow"):
    id: int
    user_id: int
    name: str
    awarded_on_url: str
    awardable_type: str


class GLEmojiMRAttributes(BaseModel, extra="allow"):
    id: int  # ID is this instance wide merge request id
    iid: int  # IID is this project's merge request id
    title: str
    created_at: str
    draft: bool
    state: str
    url: str

    # https://docs.gitlab.com/ee/api/merge_requests.html#merge-status
    detailed_merge_status: str  # mergeable, not_approved

    head_pipeline_id: int | None
    work_in_progress: bool
    source_project_id: int
    source_branch: str
    target_project_id: int
    target_branch: str


class EmojiPayload(BaseModel, extra="allow"):
    object_kind: Literal["emoji"]
    event_type: Literal["award"] | Literal["revoke"]
    user: GLUser
    object_attributes: GLEmojiAttributes
    merge_request: GLEmojiMRAttributes
