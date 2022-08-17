from __future__ import annotations

import functools
from contextlib import contextmanager
from typing import Any, Callable, Generator, Iterable, Tuple
from unittest import TestCase

import pytest
from django.conf import settings
from django.test import override_settings

from sentry.servermode import ServerComponentMode

TestMethod = Callable[..., None]
TestMethodPredicate = Callable[[TestMethod], bool]


class ServerModeTest:
    """Decorate a test case that is expected to work in a given server mode.

    By default, the test is executed if the environment is in that server mode
    or in monolith mode. The test is skipped in an incompatible mode.

    If the SERVER_COMPONENT_MODE_SPLICE_TESTS environment flag is set, any
    decorated test class will be modified by having new test methods inserted.
    These new methods run in the given modes and have generated names (such as
    "test_response__in_customer_silo"). This can be used in a dev environment to
    test in multiple modes conveniently during a single test run. Individually
    decorated methods and stand-alone functions are treated as normal.
    """

    def __init__(self, *server_modes: ServerComponentMode) -> None:
        self.server_modes = frozenset(server_modes)
        self.splice = bool(settings.SERVER_COMPONENT_MODE_SPLICE_TESTS)

    @staticmethod
    def _find_all_test_methods(test_class: type) -> Iterable[Tuple[str, TestMethod]]:
        for attr_name in dir(test_class):
            if attr_name.startswith("test_"):
                attr = getattr(test_class, attr_name)
                if callable(attr):
                    yield attr_name, attr

    def _create_mode_methods_to_splice(
        self, test_method: TestMethod
    ) -> Iterable[Tuple[str, TestMethod]]:
        for mode in self.server_modes:

            def replacement_test_method(*args: Any, **kwargs: Any) -> None:
                with override_settings(SERVER_COMPONENT_MODE=mode):
                    test_method(*args, **kwargs)

            functools.update_wrapper(replacement_test_method, test_method)
            modified_name = f"{test_method.__name__}__in_{str(mode).lower()}_silo"
            replacement_test_method.__name__ = modified_name
            yield modified_name, replacement_test_method

    def _splice_mode_methods(self, test_class: type) -> type:
        for (method_name, test_method) in self._find_all_test_methods(test_class):
            for (new_name, new_method) in self._create_mode_methods_to_splice(test_method):
                setattr(test_class, new_name, new_method)
        return test_class

    def __call__(self, decorated_obj: Any) -> Any:
        is_test_case_class = isinstance(decorated_obj, type) and issubclass(decorated_obj, TestCase)
        is_function = callable(decorated_obj)
        if not (is_test_case_class or is_function):
            raise ValueError("@ServerModeTest must decorate a function or TestCase class")

        if self.splice and is_test_case_class:
            return self._splice_mode_methods(decorated_obj)

        current_server_mode = ServerComponentMode.get_current_mode()
        is_skipped = (
            current_server_mode != ServerComponentMode.MONOLITH
            and current_server_mode not in self.server_modes
        )
        reason = f"Test case is not part of {current_server_mode} mode"
        return pytest.mark.skipif(is_skipped, reason=reason)(decorated_obj)


control_silo_test = ServerModeTest(ServerComponentMode.CONTROL)
customer_silo_test = ServerModeTest(ServerComponentMode.CUSTOMER)


@contextmanager
def exempt_from_mode_limits() -> Generator[None, None, None]:
    """Exempt test setup code from server mode checks.

    This can be used to decorate functions that are used exclusively in setting
    up test cases, so that those functions don't produce false exceptions from
    writing to tables that wouldn't be allowed in a certain ServerModeTest case.

    It can also be used as a context manager to enclose setup code within a test
    method. Such setup code would ideally be moved to the test class's `setUp`
    method or a helper function where possible, but this is available as a
    kludge when that's too inconvenient. For example:

    ```
    @ServerModeTest(ServerComponentMode.CUSTOMER)
    class MyTest(TestCase):
        def test_something(self):
            with exempt_from_mode_limits():
                org = self.create_organization()  # would be wrong if under test
            do_something(org)  # the actual code under test
    ```
    """
    with override_settings(SERVER_COMPONENT_MODE=ServerComponentMode.MONOLITH):
        yield
