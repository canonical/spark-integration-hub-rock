# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
import pytest

from monitor_sa import SANames, build_patterns, is_allowed


@pytest.mark.parametrize(
    ["raw_service_account", "allowlist", "expected"],
    [
        # Explicit listing
        ("foo:bar", ["foo:bar"], True),
        ("buzz:bizz", ["foo:bar", "buzz:bizz"], True),
        ("foo:bar", ["foo:barracuda"], False),
        # Wildcards
        ("foo:bar", ["foo:*"], True),
        ("foo:bar", ["*:bar"], True),
        ("foo:bar", ["*:barracuda"], False),
        # Prefixes/suffixes
        ("foo:barracuda", ["foo:bar*"], True),
        ("foo-fighters:barracuda", ["foo-*:bar*"], True),
        ("foo:barracuda", ["*:*cuda"], True),
        # Any
        ("foo:bar", ["*:*"], True),
    ],
)
def test_is_allowed(raw_service_account: str, allowlist: list[str], expected: bool) -> None:
    # Given
    patterns = build_patterns(allowlist)
    ns, _, sa = raw_service_account.partition(":")
    service_account = SANames(ns, sa)

    # When
    result = is_allowed(service_account, patterns)

    # Then
    assert result is expected
