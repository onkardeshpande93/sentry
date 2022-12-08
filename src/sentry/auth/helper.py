from __future__ import annotations

import logging
from dataclasses import dataclass
from functools import cached_property
from typing import Any, Collection, Dict, Mapping, Sequence, Tuple, cast

import sentry_sdk
from django.conf import settings
from django.contrib import messages
from django.contrib.auth.models import AnonymousUser
from django.db import transaction
from django.db.models import F
from django.http import HttpResponseRedirect
from django.http.request import HttpRequest
from django.http.response import HttpResponse, HttpResponseBase
from django.urls import reverse
from django.utils import timezone
from django.utils.translation import ugettext_lazy as _
from django.views import View

from sentry import audit_log, features
from sentry.api.invite_helper import ApiInviteHelper, remove_invite_details_from_session
from sentry.api.utils import generate_organization_url
from sentry.auth.exceptions import IdentityNotValid
from sentry.auth.idpmigration import (
    get_verification_value_from_key,
    send_one_time_account_confirm_link,
)
from sentry.auth.provider import MigratingIdentityId, Provider
from sentry.auth.superuser import is_active_superuser
from sentry.locks import locks
from sentry.models import AuthIdentity, AuthProvider, Organization, OrganizationMember, User
from sentry.pipeline import Pipeline, PipelineSessionStore
from sentry.pipeline.provider import PipelineProvider
from sentry.services.hybrid_cloud.audit import AuditLogMetadata, audit_log_service
from sentry.services.hybrid_cloud.auth import ApiAuthIdentity, auth_service
from sentry.services.hybrid_cloud.email import AmbiguousUserFromEmail, email_service
from sentry.services.hybrid_cloud.organization import (
    ApiOrganization,
    ApiOrganizationMember,
    ApiOrganizationMemberFlags,
    organization_service,
)
from sentry.services.hybrid_cloud.user import APIUser
from sentry.signals import sso_enabled, user_signup
from sentry.tasks.auth import email_missing_links
from sentry.utils import auth, json, metrics
from sentry.utils.audit import create_audit_entry
from sentry.utils.hashlib import md5_text
from sentry.utils.http import absolute_uri
from sentry.utils.retries import TimedRetryPolicy
from sentry.utils.session_store import redis_property
from sentry.utils.urls import add_params_to_url
from sentry.web.forms.accounts import AuthenticationForm
from sentry.web.helpers import render_to_response

from . import manager

logger = logging.getLogger("sentry.auth")

OK_LINK_IDENTITY = _("You have successfully linked your account to your SSO provider.")

OK_SETUP_SSO = _(
    "SSO has been configured for your organization and any existing members have been sent an email to link their accounts."
)

ERR_UID_MISMATCH = _("There was an error encountered during authentication.")

ERR_NOT_AUTHED = _("You must be authenticated to link accounts.")

ERR_INVALID_IDENTITY = _("The provider did not return a valid user identity.")


class AuthHelperSessionStore(PipelineSessionStore):
    redis_namespace = "auth"

    @property
    def session_key(self) -> str:
        return "auth_key"

    flow = redis_property("flow")

    def mark_session(self) -> None:
        super().mark_session()
        self.request.session.modified = True

    def is_valid(self) -> bool:
        return super().is_valid() and self.flow in (
            AuthHelper.FLOW_LOGIN,
            AuthHelper.FLOW_SETUP_PROVIDER,
        )


@dataclass
class AuthIdentityHandler:
    # SSO auth handler

    auth_provider: AuthProvider
    provider: Provider
    organization: ApiOrganization
    request: HttpRequest
    identity: Mapping[str, Any]

    @cached_property
    def user(self) -> User | APIUser | AnonymousUser:
        email = self.identity.get("email")
        if email:
            try:
                user = email_service.resolve_email_to_user(email)
            except AmbiguousUserFromEmail as e:
                user = e.users[0]
                self.warn_about_ambiguous_email(email, e.users, user)
            if user is not None:
                return user
        return self.request.user

    @staticmethod
    def warn_about_ambiguous_email(email: str, users: Collection[User], chosen_user: User) -> None:
        with sentry_sdk.push_scope() as scope:
            scope.level = "warning"
            scope.set_tag("email", email)
            scope.set_extra("user_ids", [user.id for user in users])
            scope.set_extra("chosen_user", chosen_user.id)
            sentry_sdk.capture_message("Handling identity from ambiguous email address")

    class _NotCompletedSecurityChecks(Exception):
        pass

    def _login(self, user: Any) -> None:
        metrics.incr(
            "sso.login_attempt",
            tags={
                "provider": self.provider.key,
                "organization_id": self.organization.id,
                "user_id": user.id,
            },
            sample_rate=1.0,
            skip_internal=False,
        )
        user_was_logged_in = auth.login(
            self.request,
            user,
            after_2fa=self.request.build_absolute_uri(),
            organization_id=self.organization.id,
        )
        if not user_was_logged_in:
            raise self._NotCompletedSecurityChecks()

        metrics.incr(
            "sso.login_success",
            tags={
                "provider": self.provider.key,
                "organization_id": self.organization.id,
                "user_id": user.id,
            },
            sample_rate=1.0,
            skip_internal=False,
        )

    @staticmethod
    def _set_linked_flag(member: ApiOrganizationMember) -> None:
        if member.flags.sso__invalid or not member.flags.sso__linked:
            member.flags.sso__invalid = False
            member.flags.sso__linked = True

            organization_service.update_membership_flags(organization_member=member)

    def handle_existing_identity(
        self,
        state: AuthHelperSessionStore,
        auth_identity: AuthIdentity,
    ) -> HttpResponseRedirect:
        # TODO(dcramer): this is very similar to attach
        now = timezone.now()
        auth_identity.update(
            data=self.provider.update_identity(
                new_data=self.identity.get("data", {}), current_data=auth_identity.data
            ),
            last_verified=now,
            last_synced=now,
        )

        member = organization_service.check_membership_by_id(
            organization_id=self.organization.id, user_id=auth_identity.user.id
        )
        if member is None:
            # this is likely the case when someone was removed from the org
            # but still has access to rejoin
            member = self._handle_new_membership(auth_identity)
        else:
            self._set_linked_flag(member)

        user = auth_identity.user
        user.backend = settings.AUTHENTICATION_BACKENDS[0]

        data = state.data
        subdomain = None
        if data:
            subdomain = data.get("subdomain") or None
        if features.has("organizations:customer-domains", self.organization, actor=user):
            subdomain = self.organization.slug

        try:
            self._login(user)
        except self._NotCompletedSecurityChecks:
            return HttpResponseRedirect(self._get_login_redirect(subdomain))

        state.clear()

        if not is_active_superuser(self.request):
            # set activeorg to ensure correct redirect upon logging in
            auth.set_active_org(self.request, self.organization.slug)
        return HttpResponseRedirect(self._get_login_redirect(subdomain))

    def _get_login_redirect(self, subdomain: str | None) -> str:
        # TODO(domains) Passing this method the organization should let us consolidate and simplify subdomain
        # state tracking.
        login_redirect_url = auth.get_login_redirect(self.request)
        if subdomain is not None:
            url_prefix = generate_organization_url(subdomain)
            login_redirect_url = absolute_uri(login_redirect_url, url_prefix=url_prefix)
        return login_redirect_url

    def _handle_new_membership(
        self, auth_identity: ApiAuthIdentity
    ) -> ApiOrganizationMember | None:
        user = auth_identity.user

        # If the user is either currently *pending* invite acceptance (as indicated
        # from the invite token and member id in the session) OR an existing invite exists on this
        # organization for the email provided by the identity provider.
        invite_helper = ApiInviteHelper.from_session_or_email(
            request=self.request, organization=self.organization, email=user.email
        )

        # If we are able to accept an existing invite for the user for this
        # organization, do so, otherwise handle new membership
        if invite_helper:
            if invite_helper.invite_approved:
                return invite_helper.accept_invite(user)

            # It's possible the user has an _invite request_ that hasn't been approved yet,
            # and is able to join the organization without an invite through the SSO flow.
            # In that case, delete the invite request and create a new membership.
            invite_helper.handle_invite_not_approved()

        flags = ApiOrganizationMemberFlags(sso__linked=True)
        # if the org doesn't have the ability to add members then anyone who got added
        # this way should be disabled until the org upgrades
        if not features.has("organizations:invite-members", self.organization):
            flags.member_limit__restricted = True

        # Otherwise create a new membership
        om = organization_service.add_organization_member(
            organization=self.organization,
            role=self.organization.default_role,
            user=user,
            flags=flags,
        )

        default_teams = self.auth_provider.default_teams.all()
        for team in default_teams:
            organization_service.add_team_member(team=team, organization_member=om)

        audit_log_service.log_organization_membership(
            metadata=AuditLogMetadata(
                organization=self.organization,
                actor=user,
                ip_address=self.request.META["REMOTE_ADDR"],
                target_object=om.id,
                target_user=om.user_id,
                event=audit_log.get_event_id("MEMBER_ADD"),
            ),
            organization_member=om,
        )

        return om

    def _get_auth_identity(self, **params: Any) -> AuthIdentity | None:
        try:
            return AuthIdentity.objects.get(auth_provider=self.auth_provider, **params)
        except AuthIdentity.DoesNotExist:
            return None

    @transaction.atomic  # type: ignore
    def handle_attach_identity(self, member: OrganizationMember | None = None) -> AuthIdentity:
        """
        Given an already authenticated user, attach or re-attach an identity.
        """
        # prioritize identifying by the SSO provider's user ID
        auth_identity = self._get_auth_identity(ident=self.identity["id"])
        if auth_identity is None:
            # otherwise look for an already attached identity
            # this can happen if the SSO provider's internal ID changes
            auth_identity = self._get_auth_identity(user=self.user)

        if auth_identity is None:
            auth_is_new = True
            auth_identity = AuthIdentity.objects.create(
                auth_provider=self.auth_provider,
                user=self.user,
                ident=self.identity["id"],
                data=self.identity.get("data", {}),
            )
        else:
            auth_is_new = False

            # TODO(dcramer): this might leave the user with duplicate accounts,
            # and in that kind of situation its very reasonable that we could
            # test email addresses + is_managed to determine if we can auto
            # merge
            if auth_identity.user != self.user:
                wipe = self._wipe_existing_identity(auth_identity)
            else:
                wipe = None

            now = timezone.now()
            auth_identity.update(
                user=self.user,
                ident=self.identity["id"],
                data=self.provider.update_identity(
                    new_data=self.identity.get("data", {}), current_data=auth_identity.data
                ),
                last_verified=now,
                last_synced=now,
            )

            logger.info(
                "sso.login-pipeline.attach-existing-identity",
                extra={
                    "wipe_result": repr(wipe),
                    "organization_id": self.organization.id,
                    "user_id": self.user.id,
                    "auth_identity_user_id": auth_identity.user.id,
                    "auth_provider_id": self.auth_provider.id,
                    "idp_identity_id": self.identity["id"],
                    "idp_identity_email": self.identity.get("email"),
                },
            )

        if member is None:
            member = self._get_organization_member(auth_identity)
        self._set_linked_flag(member)

        if auth_is_new:
            audit_log_service.log_auth_identity(
                metadata=AuditLogMetadata(
                    organization=self.organization,
                    actor=self.user,
                    ip_address=self.request.META["REMOTE_ADDR"],
                    target_object=auth_identity.id,
                    event=audit_log.get_event_id("SSO_IDENTITY_LINK"),
                ),
                auth_identity=auth_identity,
            )

            messages.add_message(self.request, messages.SUCCESS, OK_LINK_IDENTITY)

        return auth_identity

    def _wipe_existing_identity(self, auth_identity: AuthIdentity) -> Any:
        # it's possible the user has an existing identity, let's wipe it out
        # so that the new identifier gets used (other we'll hit a constraint)
        # violation since one might exist for (provider, user) as well as
        # (provider, ident)
        deletion_result = (
            AuthIdentity.objects.exclude(id=auth_identity.id)
            .filter(auth_provider=self.auth_provider, user=self.user)
            .delete()
        )

        # since we've identified an identity which is no longer valid
        # lets preemptively mark it as such
        other_member = organization_service.check_membership_by_id(
            organization_id=self.organization.id, user_id=auth_identity.user_id
        )
        if other_member is None:
            return
        other_member.flags.sso__invalid = True
        other_member.flags.sso__linked = False
        organization_service.update_membership_flags(organization_member=other_member)

        return deletion_result

    def _get_organization_member(self, auth_identity: AuthIdentity) -> ApiOrganizationMember:
        """
        Check to see if the user has a member associated, if not, create a new membership
        based on the auth_identity email.
        """
        member = organization_service.check_membership_by_id(
            organization_id=self.organization.id, user_id=self.user.id
        )
        if member is None:
            return self._handle_new_membership(auth_identity)
        return member

    def _respond(
        self,
        template: str,
        context: Mapping[str, Any] | None = None,
        status: int = 200,
    ) -> HttpResponse:
        default_context = {"organization": self.organization}
        if context:
            default_context.update(context)

        return render_to_response(template, default_context, self.request, status=status)

    def _post_login_redirect(self) -> HttpResponseRedirect:
        url = auth.get_login_redirect(self.request)
        if self.request.POST.get("op") == "newuser":
            # add events that we can handle on the front end
            provider = self.auth_provider.provider if self.auth_provider else None
            params = {
                "frontend_events": json.dumps({"event_name": "Sign Up", "event_label": provider})
            }
            url = add_params_to_url(url, params)
        response = HttpResponseRedirect(url)

        # Always remove any pending invite cookies, pending invites will have been
        # accepted during the SSO flow.
        remove_invite_details_from_session(self.request)

        return response

    def has_verified_account(self, verification_value: Dict[str, Any]) -> bool:
        return bool(
            verification_value["email"] == self.identity["email"]
            and verification_value["user_id"] == self.user.id
        )

    @property
    def _logged_in_user(self) -> User | None:
        """The user, if they have authenticated on this session."""
        return self.request.user if self.request.user.is_authenticated else None

    @property
    def _app_user(self) -> User | None:
        """The user, if they are represented persistently in our app."""
        return self.user if isinstance(self.user, User) else None

    def _has_usable_password(self) -> bool:
        return bool(self._app_user and self._app_user.has_usable_password())

    def handle_unknown_identity(
        self,
        state: AuthHelperSessionStore,
    ) -> HttpResponse:
        """
        Flow is activated upon a user logging in to where an AuthIdentity is
        not present.

        XXX(dcramer): this docstring is out of date

        The flow will attempt to answer the following:

        - Is there an existing user with the same email address? Should they be
          merged?

        - Is there an existing user (via authentication) that should be merged?

        - Should I create a new user based on this identity?
        """
        op = self.request.POST.get("op")
        login_form = (
            None
            if self._logged_in_user
            else AuthenticationForm(
                self.request,
                self.request.POST if self.request.POST.get("op") == "login" else None,
                initial={"username": self._app_user and self._app_user.username},
            )
        )
        # we don't trust all IDP email verification, so users can also confirm via one time email link
        is_account_verified = False
        if self.request.session.get("confirm_account_verification_key"):
            verification_key = self.request.session.get("confirm_account_verification_key")
            verification_value = get_verification_value_from_key(verification_key)
            if verification_value:
                is_account_verified = self.has_verified_account(verification_value)

        is_new_account = not self.user.is_authenticated  # stateful
        if self._app_user and self.identity.get("email_verified") or is_account_verified:
            # we only allow this flow to happen if the existing user has
            # membership, otherwise we short circuit because it might be
            # an attempt to hijack membership of another organization
            has_membership = OrganizationMember.objects.filter(
                user=self._app_user, organization=self.organization
            ).exists()
            if has_membership:
                try:
                    self._login(self.user)
                except self._NotCompletedSecurityChecks:
                    # adding is_account_verified to the check below in order to redirect
                    # to 2fa when the user migrates their idp but has 2fa enabled,
                    # otherwise it would stop them from linking their sso provider
                    if self._has_usable_password() or is_account_verified:
                        return self._post_login_redirect()
                    else:
                        is_new_account = True
                else:
                    # assume they've confirmed they want to attach the identity
                    op = "confirm"
            elif is_account_verified:
                op = "confirm"
            else:
                # force them to create a new account
                is_new_account = True
        # without a usable password they can't login, so default to a new account
        elif not self._has_usable_password():
            is_new_account = True

        auth_identity = None
        if op == "confirm" and self.user.is_authenticated or is_account_verified:
            auth_identity = self.handle_attach_identity()
        elif op == "newuser":
            auth_identity = self.handle_new_user()
        elif op == "login" and not self._logged_in_user:
            # confirm authentication, login
            assert login_form is not None
            op = None
            if login_form.is_valid():
                # This flow is special.  If we are going through a 2FA
                # flow here (login returns False) we want to instruct the
                # system to return upon completion of the 2fa flow to the
                # current URL and continue with the dialog.
                #
                # If there is no 2fa we don't need to do this and can just
                # go on.
                try:
                    self._login(login_form.get_user())
                except self._NotCompletedSecurityChecks:
                    return self._post_login_redirect()
            else:
                auth.log_auth_failure(self.request, self.request.POST.get("username"))
        else:
            op = None

        if not op:
            existing_user, template = self._dispatch_to_confirmation(is_new_account)

            context = {
                "identity": self.identity,
                "provider": self.provider_name,
                "identity_display_name": self.identity.get("name") or self.identity.get("email"),
                "identity_identifier": self.identity.get("email") or self.identity.get("id"),
                "existing_user": existing_user,
            }
            if login_form:
                context["login_form"] = login_form
            return self._respond(f"sentry/{template}.html", context)

        assert auth_identity is not None
        user = auth_identity.user
        user.backend = settings.AUTHENTICATION_BACKENDS[0]

        # XXX(dcramer): this is repeated from above
        try:
            self._login(user)
        except self._NotCompletedSecurityChecks:
            return self._post_login_redirect()

        state.clear()

        if not is_active_superuser(self.request):
            auth.set_active_org(self.request, self.organization.slug)
        return self._post_login_redirect()

    @property
    def provider_name(self) -> str:
        if self.auth_provider:
            return cast(str, self.auth_provider.provider_name)
        else:
            # A blank character is needed to prevent an HTML span from collapsing
            return " "

    def _dispatch_to_confirmation(self, is_new_account: bool) -> Tuple[User | None, str]:
        if self._logged_in_user:
            return self._logged_in_user, "auth-confirm-link"

        if self._app_user and not self._has_usable_password():
            send_one_time_account_confirm_link(
                self._app_user,
                self.organization,
                self.auth_provider,
                self.identity["email"],
                self.identity["id"],
            )
            return self.user, "auth-confirm-account"

        self.request.session.set_test_cookie()
        return None if is_new_account else self.user, "auth-confirm-identity"

    def handle_new_user(self) -> ApiAuthIdentity:
        user, auth_identity = auth_service.provision_user_from_sso(
            auth_provider=self.auth_provider, identity_data=self.identity
        )

        provider = self.auth_provider.provider if self.auth_provider else None
        user_signup.send_robust(
            sender=self.handle_new_user,
            user=user,
            source="sso",
            provider=provider,
            referrer="in-app",
        )

        self._handle_new_membership(auth_identity)

        return auth_identity


class AuthHelper(Pipeline):
    """
    Helper class which is passed into AuthView's.

    Designed to link provider and views as well as manage the state and
    pipeline. The pipeline is optional, in case can be done

    Auth has several flows:

    1. The user is going through provider setup, thus enforcing that they link
       their current account to the new auth identity.
    2. The user is anonymous and creating a brand new account.
    3. The user is anonymous and logging into an existing account.
    4. The user is anonymous and creating a brand new account, but may have an
       existing account that should/could be merged.
    5. The user is authenticated and creating a new identity, thus associating
       it with their current account.
    6. The user is authenticated and creating a new identity, but not linking
       it with their account (thus creating a new account).
    """

    # logging in or registering
    FLOW_LOGIN = 1
    # configuring the provider
    FLOW_SETUP_PROVIDER = 2

    pipeline_name = "pipeline"
    provider_manager = manager
    provider_model_cls = AuthProvider
    session_store_cls = AuthHelperSessionStore

    @classmethod
    def get_for_request(cls, request: HttpRequest) -> AuthHelper | None:
        req_state = cls.unpack_state(request)
        if not req_state:
            return None

        if not req_state.organization:
            logging.info("Invalid SSO data found")
            return None
        flow = req_state.state.flow

        return cls(
            request,
            req_state.organization,
            flow,
            auth_provider=req_state.provider_model,
            provider_key=req_state.provider_key,
        )

    def __init__(
        self,
        request: HttpRequest,
        organization: Organization,
        flow: int,
        auth_provider: AuthProvider | None = None,
        provider_key: str | None = None,
    ) -> None:
        assert provider_key or auth_provider
        self.flow = flow

        # TODO: Resolve inconsistency with nullable provider_key.
        # Tagging with "type: ignore" because the superclass requires provider_key to
        # be non-nullable. We get away with it because super().__init__ only passes
        # provider_key to get_provider, and our get_provider override accepts a null
        # provider_key. But it technically violates the type contract and we'll need
        # to change the superclass to accommodate this one.
        super().__init__(request, provider_key, organization, auth_provider)  # type: ignore

        # Override superclass's type hints to be narrower
        self.organization: Organization = self.organization
        self.provider: Provider = self.provider

    def get_provider(self, provider_key: str | None) -> PipelineProvider:
        if self.provider_model:
            return cast(PipelineProvider, self.provider_model.get_provider())
        elif provider_key:
            return super().get_provider(provider_key)
        else:
            raise NotImplementedError

    def get_pipeline_views(self) -> Sequence[View]:
        assert isinstance(self.provider, Provider)
        if self.flow == self.FLOW_LOGIN:
            return self.provider.get_auth_pipeline()
        elif self.flow == self.FLOW_SETUP_PROVIDER:
            return self.provider.get_setup_pipeline()
        else:
            raise NotImplementedError

    def is_valid(self) -> bool:
        return super().is_valid() and self.state.flow in (self.FLOW_LOGIN, self.FLOW_SETUP_PROVIDER)

    def get_initial_state(self) -> Mapping[str, Any]:
        state = dict(super().get_initial_state())
        state.update({"flow": self.flow})
        return state

    def get_redirect_url(self) -> str:
        return absolute_uri(reverse("sentry-auth-sso"))

    def dispatch_to(self, step: View) -> HttpResponseBase:
        return step.dispatch(request=self.request, helper=self)

    def finish_pipeline(self) -> HttpResponseBase:
        data = self.fetch_state()

        # The state data may have expired, in which case the state data will
        # simply be None.
        if not data:
            return self.error(ERR_INVALID_IDENTITY)

        try:
            identity = self.provider.build_identity(data)
        except IdentityNotValid as error:
            return self.error(str(error) or ERR_INVALID_IDENTITY)

        if self.state.flow == self.FLOW_LOGIN:
            # create identity and authenticate the user
            response = self._finish_login_pipeline(identity)
        elif self.state.flow == self.FLOW_SETUP_PROVIDER:
            response = self._finish_setup_pipeline(identity)
        else:
            raise Exception(f"Unrecognized flow value: {self.state.flow}")

        return response

    def auth_handler(self, identity: Mapping[str, Any]) -> AuthIdentityHandler:
        return AuthIdentityHandler(
            self.provider_model, self.provider, self.organization, self.request, identity
        )

    @transaction.atomic  # type: ignore
    def _finish_login_pipeline(self, identity: Mapping[str, Any]) -> HttpResponse:
        """
        The login flow executes both with anonymous and authenticated users.

        Upon completion a few branches exist:

        If the identity is already linked, the user should be logged in
        and redirected immediately.

        Otherwise, the user is presented with a confirmation window. That window
        will show them the new account that will be created, and if they're
        already authenticated an optional button to associate the identity with
        their account.
        """
        auth_provider = self.provider_model
        assert auth_provider is not None
        user_id = identity["id"]

        lock = locks.get(
            f"sso:auth:{auth_provider.id}:{md5_text(user_id).hexdigest()}",
            duration=5,
            name="sso_auth",
        )
        with TimedRetryPolicy(5)(lock.acquire):
            try:
                auth_identity = AuthIdentity.objects.select_related("user").get(
                    auth_provider=auth_provider, ident=user_id
                )
            except AuthIdentity.DoesNotExist:
                auth_identity = None

            # Handle migration of identity keys
            if not auth_identity and isinstance(user_id, MigratingIdentityId):
                try:
                    auth_identity = AuthIdentity.objects.select_related("user").get(
                        auth_provider=auth_provider, ident=user_id.legacy_id
                    )
                    auth_identity.update(ident=user_id.id)
                except AuthIdentity.DoesNotExist:
                    auth_identity = None

            auth_handler = self.auth_handler(identity)
            if not auth_identity:
                return auth_handler.handle_unknown_identity(self.state)

            # If the User attached to this AuthIdentity is not active,
            # we want to clobber the old account and take it over, rather than
            # getting logged into the inactive account.
            if not auth_identity.user.is_active:
                # Current user is also not logged in, so we have to
                # assume unknown.
                if not self.request.user.is_authenticated:
                    return auth_handler.handle_unknown_identity(self.state)
                auth_identity = auth_handler.handle_attach_identity()

            return auth_handler.handle_existing_identity(self.state, auth_identity)

    @transaction.atomic  # type: ignore
    def _finish_setup_pipeline(self, identity: Mapping[str, Any]) -> HttpResponseRedirect:
        """
        The setup flow creates the auth provider as well as an identity linked
        to the active user.
        """
        request = self.request
        if not request.user.is_authenticated:
            return self.error(ERR_NOT_AUTHED)

        if request.user.id != self.state.uid:
            return self.error(ERR_UID_MISMATCH)

        data = self.fetch_state()
        config = self.provider.build_config(data)

        om = organization_service.check_membership_by_id(
            organization_id=self.organization.id, user_id=request.user.id
        )
        if om is None:
            return self.error(ERR_UID_MISMATCH)

        # disable require 2FA for the organization
        # since only SSO or require 2FA can be enabled
        self.disable_2fa_required()

        self.provider_model = AuthProvider.objects.create(
            organization=self.organization, provider=self.provider.key, config=config
        )

        self.auth_handler(identity).handle_attach_identity(om)

        auth.mark_sso_complete(request, self.organization.id)

        sso_enabled.send_robust(
            organization=self.organization,
            user=request.user,
            provider=self.provider.key,
            sender=self.__class__,
        )

        audit_log_service.log_auth_provider(
            metadata=AuditLogMetadata(
                organization=self.organization,
                actor=request.user,
                ip_address=request.META["REMOTE_ADDR"],
                target_object=self.provider_model.id,
                event=audit_log.get_event_id("SSO_ENABLE"),
            ),
            provider=self.provider_model,
        )

        email_missing_links.delay(self.organization.id, request.user.id, self.provider.key)

        messages.add_message(self.request, messages.SUCCESS, OK_SETUP_SSO)

        self.clear_session()

        next_uri = reverse(
            "sentry-organization-auth-provider-settings", args=[self.organization.slug]
        )
        return HttpResponseRedirect(next_uri)

    def error(self, message: str) -> HttpResponseRedirect:
        redirect_uri = "/"

        if self.state.flow == self.FLOW_LOGIN:
            # create identity and authenticate the user
            redirect_uri = reverse("sentry-auth-organization", args=[self.organization.slug])

        elif self.state.flow == self.FLOW_SETUP_PROVIDER:
            redirect_uri = reverse(
                "sentry-organization-auth-settings", args=[self.organization.slug]
            )

        if redirect_uri == "/":
            metrics.incr(
                "sso.error",
                tags={
                    "flow": self.state.flow,
                    "provider": self.provider.key,
                    "organization_id": self.organization.id,
                    "user_id": self.request.user.id,
                },
                skip_internal=False,
                sample_rate=1.0,
            )
        else:
            metrics.incr(
                "sso.exit",
                tags={
                    "flow": self.state.flow,
                    "provider": self.provider.key,
                    "organization_id": self.organization.id,
                    "user_id": self.request.user.id,
                },
                skip_internal=False,
                sample_rate=1.0,
            )

        # NOTE: Does NOT necessarily indicate a login _failure_
        logger.warning(
            "sso.login-pipeline.error",
            extra={
                "flow": self.state.flow,
                "provider": self.provider.key,
                "organization_id": self.organization.id,
                "user_id": self.request.user.id,
                "error_message": message,
            },
        )

        messages.add_message(self.request, messages.ERROR, f"Authentication error: {message}")

        return HttpResponseRedirect(redirect_uri)

    def disable_2fa_required(self) -> None:
        require_2fa = self.organization.flags.require_2fa

        if not require_2fa or not require_2fa.is_set:
            return

        self.organization.update(flags=F("flags").bitand(~Organization.flags.require_2fa))

        logger.info(
            "Require 2fa disabled during sso setup", extra={"organization_id": self.organization.id}
        )
        create_audit_entry(
            request=self.request,
            organization=self.organization,
            target_object=self.organization.id,
            event=audit_log.get_event_id("ORG_EDIT"),
            data={"require_2fa": "to False when enabling SSO"},
        )
