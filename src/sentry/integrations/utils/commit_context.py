from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import sentry_sdk

from sentry import analytics
from sentry.integrations.base import IntegrationInstallation
from sentry.integrations.mixins.commit_context import (
    CommitContextMixin,
    FileBlameInfo,
    SourceLineInfo,
)
from sentry.models.integrations.repository_project_path_config import RepositoryProjectPathConfig
from sentry.ownership.grammar import get_source_code_path_from_stacktrace_path
from sentry.services.hybrid_cloud.integration import integration_service
from sentry.shared_integrations.exceptions import ApiError
from sentry.utils.committers import get_stacktrace_path_from_event_frame

logger = logging.getLogger("sentry.tasks.process_commit_context")


def find_commit_context_for_event_all_frames(
    code_mappings: Sequence[RepositoryProjectPathConfig],
    frames: Sequence[Mapping[str, Any]],
    organization_id: int,
    project_id: int,
    extra: Mapping[str, Any],
) -> Optional[FileBlameInfo]:
    file_blames: Sequence[FileBlameInfo] = []
    integration_to_files_mapping: Dict[str, list[SourceLineInfo]] = {}

    num_successfully_mapped_frames = 0

    for frame in frames:
        for code_mapping in code_mappings:
            stacktrace_path = get_stacktrace_path_from_event_frame(frame)

            if not stacktrace_path:
                logger.info(
                    "process_commit_context_all_frames.no_stacktrace_path",
                    extra={
                        **extra,
                        "code_mapping_id": code_mapping.id,
                    },
                )
                continue

            src_path = get_source_code_path_from_stacktrace_path(stacktrace_path, code_mapping)

            # src_path can be none if the stacktrace_path is an invalid filepath
            if not src_path:
                logger.info(
                    "process_commit_context_all_frames.no_src_path",
                    extra={
                        **extra,
                        "code_mapping_id": code_mapping.id,
                        "stacktrace_path": stacktrace_path,
                    },
                )
                continue

            num_successfully_mapped_frames += 1
            logger.info(
                "process_commit_context_all_frames.found_stacktrace_and_src_paths",
                extra={
                    **extra,
                    "code_mapping_id": code_mapping.id,
                    "stacktrace_path": stacktrace_path,
                    "src_path": src_path,
                },
            )

            files = integration_to_files_mapping.setdefault(
                code_mapping.organization_integration_id, []
            )
            files.append(
                SourceLineInfo(
                    lineno=frame["lineno"],
                    path=src_path,
                    ref=code_mapping.default_branch or "master",
                    repo=code_mapping.repository,
                    code_mapping=code_mapping,
                )
            )
            break

    for integration_organziation_id, files in integration_to_files_mapping.items():
        integration = integration_service.get_integration(
            organization_integration_id=integration_organziation_id
        )
        if not integration:
            continue
        install = integration.get_installation(organization_id=organization_id)
        if not isinstance(install, CommitContextMixin):
            continue
        try:
            blames = install.get_commit_context_all_frames(files)
            file_blames.extend(blames)
        except ApiError as e:
            sentry_sdk.capture_exception(e)
            analytics.record(
                "integrations.failed_to_fetch_commit_context_all_frames",
                organization_id=organization_id,
                project_id=project_id,
                group_id=extra["group"],
                installation_id=install.model.id,
                num_frames=len(files),
                provider=integration.provider,
                error_message=e.text,
            )
            logger.error(
                "process_commit_context_all_frames.failed_to_fetch_commit_context",
                extra={
                    **extra,
                    "integration_organization_id": integration_organziation_id,
                    "error_message": e.text,
                },
            )

    most_recent_blame = max(file_blames, key=lambda blame: blame.commit.committedDate, default=None)
    # Only return suspect commits that are less than a year old
    selected_blame = (
        most_recent_blame
        if most_recent_blame and is_date_less_than_year(most_recent_blame.commit.committedDate)
        else None
    )

    _record_commit_context_all_frames_analytics(
        selected_blame=selected_blame,
        organization_id=organization_id,
        project_id=project_id,
        extra=extra,
        frames=frames,
        file_blames=file_blames,
    )

    return selected_blame


def find_commit_context_for_event(
    code_mappings: Sequence[RepositoryProjectPathConfig],
    frame: Mapping[str, Any],
    extra: Mapping[str, Any],
) -> tuple[List[Tuple[Mapping[str, Any], RepositoryProjectPathConfig]], IntegrationInstallation]:
    """

    Get all the Commit Context for an event frame using a source code integration for all the matching code mappings
    code_mappings: List of RepositoryProjectPathConfig
    frame: Event frame
    """
    result = []
    installation = None
    for code_mapping in code_mappings:
        if not code_mapping.organization_integration_id:
            logger.info(
                "process_commit_context.no_integration",
                extra={
                    **extra,
                    "code_mapping_id": code_mapping.id,
                },
            )
            continue

        stacktrace_path = get_stacktrace_path_from_event_frame(frame)

        if not stacktrace_path:
            logger.info(
                "process_commit_context.no_stacktrace_path",
                extra={
                    **extra,
                    "code_mapping_id": code_mapping.id,
                },
            )
            continue

        src_path = get_source_code_path_from_stacktrace_path(stacktrace_path, code_mapping)

        # src_path can be none if the stacktrace_path is an invalid filepath
        if not src_path:
            logger.info(
                "process_commit_context.no_src_path",
                extra={
                    **extra,
                    "code_mapping_id": code_mapping.id,
                    "stacktrace_path": stacktrace_path,
                },
            )
            continue

        logger.info(
            "process_commit_context.found_stacktrace_and_src_paths",
            extra={
                **extra,
                "code_mapping_id": code_mapping.id,
                "stacktrace_path": stacktrace_path,
                "src_path": src_path,
            },
        )
        integration = integration_service.get_integration(
            organization_integration_id=code_mapping.organization_integration_id
        )
        install = integration.get_installation(organization_id=code_mapping.organization_id)
        if installation is None and install is not None:
            installation = install
        try:
            commit_context = install.get_commit_context(
                code_mapping.repository, src_path, code_mapping.default_branch, frame
            )
        except ApiError as e:
            commit_context = None
            sentry_sdk.capture_exception(e)
            analytics.record(
                "integrations.failed_to_fetch_commit_context",
                organization_id=code_mapping.organization_id,
                project_id=code_mapping.project.id,
                group_id=extra["group"],
                code_mapping_id=code_mapping.id,
                provider=integration.provider,
                error_message=e.text,
            )
            logger.error(
                "process_commit_context.failed_to_fetch_commit_context",
                extra={
                    **extra,
                    "code_mapping_id": code_mapping.id,
                    "stacktrace_path": stacktrace_path,
                    "src_path": src_path,
                    "error_message": e.text,
                },
            )

        # Only return suspect commits that are less than a year old
        if commit_context and is_date_less_than_year(commit_context["committedDate"]):
            result.append((commit_context, code_mapping))

    return result, installation


def is_date_less_than_year(date: datetime):
    return date > datetime.now(tz=timezone.utc) - timedelta(days=365)


def _record_commit_context_all_frames_analytics(
    selected_blame: Optional[FileBlameInfo],
    organization_id: int,
    project_id: int,
    extra: Mapping[str, Any],
    frames: Sequence[Mapping[str, Any]],
    file_blames: Sequence[FileBlameInfo],
):
    if not selected_blame:
        return

    unique_commit_ids = {blame.commit.commitId for blame in file_blames}
    unique_author_emails = {blame.commit.commitAuthorEmail for blame in file_blames}
    selected_frame_index = next(
        (
            i
            for i, frame in enumerate(frames)
            if frame["lineno"] == selected_blame.lineno
            and get_source_code_path_from_stacktrace_path(
                get_stacktrace_path_from_event_frame(frame) or "", selected_blame.code_mapping
            )
            == selected_blame.path
        ),
        None,
    )

    analytics.record(
        "integrations.successfully_fetched_commit_context_all_frames",
        organization_id=organization_id,
        project_id=project_id,
        group_id=extra["group"],
        num_frames=len(frames),
        num_unique_commits=len(unique_commit_ids),
        num_unique_commit_authors=len(unique_author_emails),
        selected_frame_index=selected_frame_index,
        selected_provider=selected_blame.repo.provider,
        selected_code_mapping_id=selected_blame.code_mapping.id,
    )
