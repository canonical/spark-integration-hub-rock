#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Routine that updates secrets for Spark service accounts."""

import base64
import fnmatch
import logging
import os
import re
import sys
from pathlib import Path
from typing import NamedTuple, cast

import httpx2
from lightkube import Client
from lightkube.core.resource import NamespacedResource
from lightkube.exceptions import ApiError
from lightkube.models.meta_v1 import ObjectMeta
from lightkube.resources.core_v1 import Secret
from spark8t.domain import PropertyFile

from app.constants import MANAGED_BY_INTEGRATION_HUB, MANAGED_BY_LABEL
from app.models import AuthorizationPolicy

logger = logging.getLogger(__name__)
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] (%(threadName)s) (%(funcName)s) %(message)s",
)


class ServiceAccountNames(NamedTuple):
    """Service Account denomination."""

    namespace: str
    name: str


class ServiceAccountPatterns(NamedTuple):
    """Service account shell-style patterns for the namespace and the actual resource name."""

    namespace: str
    name: str


def read_configuration_file(file_path: str) -> dict[str, str]:
    """Read spark configuration file."""
    if not os.path.exists(file_path):
        return {}
    return PropertyFile.read(file_path).props


def build_patterns(allowlist: list[str]) -> list[ServiceAccountPatterns]:
    """Build shell-style patterns from allowlist."""
    patterns = []
    for entry in allowlist:
        ns, _, sa = entry.partition(":")
        patterns.append(ServiceAccountPatterns(fnmatch.translate(ns), fnmatch.translate(sa)))

    return patterns


def is_allowed(
    service_account: ServiceAccountNames, patterns: list[ServiceAccountPatterns]
) -> bool:
    """Compare a service account against a list of shell-style patterns."""
    return any(
        re.match(sa_patterns.namespace, service_account.namespace)
        and re.match(sa_patterns.name, service_account.name)
        for sa_patterns in patterns
    )


def create_secret_from_file(secret_name: str, file_path: Path, namespace: str) -> Secret:
    """Create a Kubernetes Secret object from a file."""
    # Read the file content
    with file_path.open("rb") as f:
        file_content = f.read()

    # The output needs to be a decoded utf-8 string for the JSON serialization
    encoded_content = base64.b64encode(file_content).decode("utf-8")

    # Construct the Secret object
    secret = Secret(
        metadata=ObjectMeta(
            name=secret_name,
            namespace=namespace,
            labels={"app.kubernetes.io/managed-by": "integration-hub"},
        ),
        type="Opaque",
        data={file_path.name: encoded_content},
    )

    return secret


def get_allowlist(file_path: Path) -> list[str]:
    """Get the service accounts allowlist from a file.

    Args:
        file_path (Path): The path to the allowlist file.

    Returns:
        list[str]: The list of allowed service accounts.
    """
    allowlist: list[str] = []
    try:
        with file_path.open("r") as f:
            allowlist = [entry.strip() for entry in f.read().splitlines()]
    except (FileNotFoundError, IsADirectoryError):
        # IsADirectoryError happens when the env var is not defined:
        # Path("") is Path(".")
        logger.warning("Did not find allowlist, proceeding without it...")
        allowlist = []
    return allowlist


def get_integration_hub_secret(
    secret_name: str, namespace: str, options: dict[str, str]
) -> Secret:
    """Get the integration hub secret as a Kubernetes Secret object.

    Args:
        secret_name (str): The name of the secret.
        namespace (str): The namespace of the secret.
        options (dict[str, str]): The key-value pairs to include in the secret.

    Returns:
        Secret: The constructed Kubernetes Secret object.
    """
    return cast(
        Secret,
        Secret.from_dict(
            {
                "apiVersion": "v1",
                "kind": "Secret",
                "metadata": {
                    "name": secret_name,
                    "namespace": namespace,
                    "labels": {MANAGED_BY_LABEL: MANAGED_BY_INTEGRATION_HUB},
                },
                "stringData": options if options else {},
            }
        ),
    )


def delete_resource_if_exists(
    client: Client,
    resource_type: type[NamespacedResource],
    namespace: str,
    resource_name: str,
):
    """Delete a Kubernetes resource if it exists.

    Args:
        client (Client): The Lightkube client instance.
        resource_type (GenericNamespacedResource): The type of the resource to delete.
        namespace (str): The namespace of the resource.
        resource_name (str): The name of the resource.
    """
    try:
        resource = client.get(resource_type, name=resource_name, namespace=namespace)
        if resource:
            client.delete(resource_type, name=resource_name, namespace=namespace)
    except (ApiError, httpx2.HTTPStatusError) as e:
        # lightkube only wraps errors as ApiError when Content-Type is exactly
        # "application/json"; other 404 responses surface as raw HTTPStatusError.
        logger.info(
            f"Api error while deleting {resource_type} named {resource_name} in namespace {namespace}: {e}"
        )
