from typing import Optional

from django.db import router
from django.db.models import Q
from rest_framework import status
from rest_framework.request import Request
from rest_framework.response import Response

from sentry.api.base import region_silo_endpoint
from sentry.api.bases.project import ProjectEndpoint, ProjectReleasePermission
from sentry.api.exceptions import ResourceDoesNotExist, SentryAPIException
from sentry.api.paginator import OffsetPaginator
from sentry.api.serializers import serialize
from sentry.api.serializers.models.artifactbundle import ArtifactBundlesSerializer
from sentry.models import ArtifactBundle, ProjectArtifactBundle
from sentry.utils.db import atomic_transaction


class InvalidSortByParameter(SentryAPIException):
    status_code = status.HTTP_400_BAD_REQUEST
    code = "invalid_sort_by_parameter"
    message = "You can either sort via 'date_added' or '-date_added'"


class ArtifactBundlesMixin:
    @classmethod
    def derive_order_by(cls, sort_by: str) -> Optional[str]:
        is_desc = sort_by.startswith("-")
        sort_by = sort_by.strip("-")

        if sort_by == "date_added":
            order_by = "date_uploaded"
            return f"-{order_by}" if is_desc else order_by

        raise InvalidSortByParameter


@region_silo_endpoint
class ArtifactBundlesEndpoint(ProjectEndpoint, ArtifactBundlesMixin):
    permission_classes = (ProjectReleasePermission,)

    def get(self, request: Request, project) -> Response:
        """
        List a Project's Artifact Bundles
        ````````````````````````````````````

        Retrieve a list of artifact bundles for a given project.

        :pparam string organization_slug: the slug of the organization the
                                          artifact bundle belongs to.
        :pparam string project_slug: the slug of the project to list the
                                     artifact bundles of.
        """
        query = request.GET.get("query")

        # TODO: since we do a left join here, in case a bundle has at least one occurrence in the
        #  ReleaseArtifactBundle table, we will not show its variant without a release. For example if a customer
        #  uploads a bundle without a release and then re-uploads the same bundle with a release. We need to see if this
        #  requires work on our end if customers do not like this behavior.
        q = Q()
        if query:
            q |= Q(bundle_id__icontains=query)
            q |= Q(
                releaseartifactbundle__isnull=False,
                releaseartifactbundle__release_name__icontains=query,
            )
            q |= Q(
                releaseartifactbundle__isnull=False,
                releaseartifactbundle__dist_name__icontains=query,
            )
        else:
            q = Q(releaseartifactbundle__isnull=False) | Q(releaseartifactbundle__isnull=True)

        try:
            queryset = ArtifactBundle.objects.filter(
                q,
                organization_id=project.organization_id,
                projectartifactbundle__project_id=project.id,
            ).values_list(
                "bundle_id",
                "releaseartifactbundle__release_name",
                "releaseartifactbundle__dist_name",
                "artifact_count",
                "date_uploaded",
            )
        except ProjectArtifactBundle.DoesNotExist:
            raise ResourceDoesNotExist

        return self.paginate(
            request=request,
            queryset=queryset,
            order_by=self.derive_order_by(sort_by=request.GET.get("sortBy", "-date_added")),
            paginator_cls=OffsetPaginator,
            default_per_page=10,
            on_results=lambda r: serialize(r, request.user, ArtifactBundlesSerializer()),
        )

    def delete(self, request: Request, project) -> Response:
        """
        Delete an Archive
        ```````````````````````````````````````````````````

        Delete all artifacts inside given archive.

        :pparam string organization_slug: the slug of the organization the
                                            archive belongs to.
        :pparam string project_slug: the slug of the project to delete the
                                        archive of.
        :qparam string name: The name of the archive to delete.
        :auth: required
        """

        bundle_id = request.GET.get("bundleId")

        if bundle_id:
            try:
                # TODO: if we delete a specific bundle with a bundle_id, do we want to do reference counting before
                #  actually deleting it? Or we just delete it and all of its release associations? This heavily depends
                #  on the UI that we will develop above.
                with atomic_transaction(using=(router.db_for_write(ArtifactBundle))):
                    ArtifactBundle.objects.get(
                        organization_id=project.organization_id, bundle_id=bundle_id
                    ).delete()

                return Response(status=204)
            except ArtifactBundle.DoesNotExist:
                return Response(
                    {"error": f"Couldn't find a bundle with bundle_id {bundle_id}"}, status=404
                )

        return Response({"error": f"Supplied an invalid bundle_id {bundle_id}"}, status=404)
