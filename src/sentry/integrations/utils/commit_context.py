from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, List, Mapping, Optional, Sequence, Tuple

import sentry_sdk

from sentry import analytics
from sentry.integrations.base import IntegrationInstallation
from sentry.integrations.mixins.commit_context import (
    CommitContextMixin,
    FileBlameInfo,
    SourceLineInfo,
)
from sentry.models.commit import Commit
from sentry.models.commitauthor import CommitAuthor
from sentry.models.integrations.repository_project_path_config import RepositoryProjectPathConfig
from sentry.ownership.grammar import get_source_code_path_from_stacktrace_path
from sentry.services.hybrid_cloud.integration import integration_service
from sentry.shared_integrations.exceptions import ApiError
from sentry.utils import metrics
from sentry.utils.committers import get_stacktrace_path_from_event_frame

logger = logging.getLogger("sentry.tasks.process_commit_context")


def find_commit_context_for_event_all_frames(
    code_mappings: Sequence[RepositoryProjectPathConfig],
    frames: Sequence[Mapping[str, Any]],
    organization_id: int,
    project_id: int,
    extra: Mapping[str, Any],
) -> tuple[Optional[FileBlameInfo], Optional[IntegrationInstallation]]:
    file_blames: list[FileBlameInfo] = []
    integration_to_files_mapping: dict[str, list[SourceLineInfo]] = {}
    integration_to_install_mapping: dict[str, IntegrationInstallation] = {}

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
        integration_to_install_mapping[integration_organziation_id] = install
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
        most_recent_blame=most_recent_blame,
        organization_id=organization_id,
        project_id=project_id,
        extra=extra,
        frames=frames,
        file_blames=file_blames,
    )

    return (
        selected_blame,
        integration_to_install_mapping[selected_blame.code_mapping.organization_integration_id]
        if selected_blame
        else None,
    )


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


def is_date_less_than_year(date: datetime) -> bool:
    return date > datetime.now(tz=timezone.utc) - timedelta(days=365)


def get_or_create_commit_from_blame(
    blame: FileBlameInfo, organization_id: int, extra: Mapping[str, str | int]
) -> Commit:
    """
    From a blame object, see if a matching commit already exists in sentry_commit.
    If not, create it.
    """
    try:
        commit = Commit.objects.get(
            repository_id=blame.repo.id,
            key=blame.commit.commitId,
        )
        if commit.message == "":
            commit.message = blame.commit.commitMessage
            commit.save()

        return commit
    except Commit.DoesNotExist:
        logger.info(
            "process_commit_context_all_frames.no_commit_in_sentry",
            extra={
                **extra,
                "sha": blame.commit.commitId,
                "repository_id": blame.repo.id,
                "code_mapping_id": blame.code_mapping.id,
                "reason": "commit_sha_does_not_exist_in_sentry",
            },
        )

        # If a commit does not exist in sentry_commit, we will add it
        commit_author, _ = CommitAuthor.objects.get_or_create(
            organization_id=organization_id,
            email=blame.commit.commitAuthorEmail,
            defaults={"name": blame.commit.commitAuthorName},
        )
        commit: Commit = Commit.objects.create(
            organization_id=organization_id,
            repository_id=blame.repo.id,
            key=blame.commit.commitId,
            date_added=blame.commit.committedDate,
            author=commit_author,
            message=blame.commit.commitMessage,
        )

        logger.info(
            "process_commit_context_all_frames.added_commit_to_sentry_commit",
            extra={
                **extra,
                "sha": blame.commit.commitId,
                "repository_id": blame.repo.id,
                "code_mapping_id": blame.code_mapping.id,
                "reason": "commit_sha_does_not_exist_in_sentry_for_all_code_mappings",
            },
        )

        return commit


def _record_commit_context_all_frames_analytics(
    selected_blame: Optional[FileBlameInfo],
    most_recent_blame: Optional[FileBlameInfo],
    organization_id: int,
    project_id: int,
    extra: Mapping[str, Any],
    frames: Sequence[Mapping[str, Any]],
    file_blames: Sequence[FileBlameInfo],
):
    if not selected_blame:
        reason = "commit_too_old" if most_recent_blame else "could_not_fetch_commit_context"
        metrics.incr(
            "sentry.tasks.process_commit_context.aborted",
            tags={
                "detail": "could_not_fetch_commit_context",
            },
        )
        logger.info(
            "process_commit_context_all_frames.find_commit_context",
            extra={
                **extra,
                "reason": reason,
                "num_frames": len(frames),
                "fallback": True,
            },
        )
        analytics.record(
            "integrations.unsuccessfully_fetched_commit_context_all_frames",
            organization_id=organization_id,
            project_id=project_id,
            group_id=extra["group"],
            num_frames=len(frames),
            reason=reason,
        )
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
