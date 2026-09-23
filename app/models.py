# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Module containing custom resource definitions."""

from typing import Type

from lightkube.generic_resource import GenericNamespacedResource, create_namespaced_resource

AuthorizationPolicy: Type[GenericNamespacedResource] = create_namespaced_resource(
    "security.istio.io",
    "v1",
    "AuthorizationPolicy",
    "authorizationpolicies",
)
