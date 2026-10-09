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
from typing import Any, Literal, NamedTuple, cast

import httpx2
from lightkube import Client
from lightkube.core.resource import NamespacedResource
from lightkube.exceptions import ApiError
from lightkube.models.meta_v1 import ObjectMeta
from lightkube.resources.core_v1 import Secret, ServiceAccount
from spark8t.domain import PropertyFile

from app.constants import (
    CLIENT_APP_NAMESPACE_LABEL,
    CLIENT_APP_SERVICE_ACCOUNT_LABEL,
    MANAGED_BY_INTEGRATION_HUB,
    MANAGED_BY_LABEL,
    WORKLOAD_NAMESPACE_LABEL,
    WORKLOAD_SERVICE_ACCOUNT_LABEL,
)
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
    secret_name: str, namespace: str, service_account: str, options: dict[str, str]
) -> Secret:
    """Get the integration hub secret as a Kubernetes Secret object.

    Args:
        secret_name (str): The name of the secret.
        namespace (str): The namespace of the secret.
        service_account (str): The workload service account the secret belongs to.
        options (dict[str, str]): The key-value pairs to include in the secret.

    Returns:
        Secret: The constructed Kubernetes Secret object.
    """
    # Use `data` (base64) rather than `stringData` so server-side apply owns and prunes
    # individual keys; `stringData` is write-only and its derived `data` keys are never
    # pruned by apply, leaving stale properties behind when the desired set shrinks.
    data = {
        key: base64.b64encode(value.encode("utf-8")).decode("utf-8")
        for key, value in (options or {}).items()
    }
    return cast(
        Secret,
        Secret.from_dict(
            {
                "apiVersion": "v1",
                "kind": "Secret",
                "metadata": {
                    "name": secret_name,
                    "namespace": namespace,
                    "labels": workload_owner_labels(namespace, service_account),
                },
                "data": data,
            }
        ),
    )


def workload_owner_labels(
    workload_namespace: str, workload_service_account: str
) -> dict[str, str]:
    """Build the labels that mark a resource as owned by a workload service account.

    Args:
        workload_namespace (str): The namespace of the workload.
        workload_service_account (str): The service account of the workload.

    Returns:
        dict[str, str]: The labels identifying the owning workload service account.
    """
    return {
        MANAGED_BY_LABEL: MANAGED_BY_INTEGRATION_HUB,
        WORKLOAD_NAMESPACE_LABEL: workload_namespace,
        WORKLOAD_SERVICE_ACCOUNT_LABEL: workload_service_account,
    }


def client_app_policy_labels(
    workload_namespace: str,
    workload_service_account: str,
    client_app_namespace: str,
    client_app_service_account: str,
) -> dict[str, str]:
    """Build the labels for an authorization policy tied to a single client application relation.

    Combines the workload ownership labels with the client application identity, so that the
    policies for one (workload, client application) relation can be computed and cleaned up
    precisely from the four identifiers alone.

    Args:
        workload_namespace (str): The namespace of the workload.
        workload_service_account (str): The service account of the workload.
        client_app_namespace (str): The namespace of the client application.
        client_app_service_account (str): The service account of the client application.

    Returns:
        dict[str, str]: The labels identifying the workload and the client application.
    """
    return {
        **workload_owner_labels(workload_namespace, workload_service_account),
        CLIENT_APP_NAMESPACE_LABEL: client_app_namespace,
        CLIENT_APP_SERVICE_ACCOUNT_LABEL: client_app_service_account,
    }


def get_workload_auth_policy(
    policy_name: str,
    workload_namespace: str,
    workload_service_account: str,
    role: Literal["driver", "executor"],
):
    """Get the base authorization policy for Spark workloads (driver and executors).

    This grants only the workload's own service account (covering driver<->executor
    traffic). Client application access to the driver is granted by separate per-relation
    policies so that client application churn never rewrites this policy.

    Args:
        policy_name (str): The name of the authorization policy.
        workload_namespace (str): The namespace of the workload.
        workload_service_account (str): The service account of the workload.
        role (Literal["driver", "executor"]): The role of the workload (driver or executor).

    Returns:
        AuthorizationPolicy: The constructed authorization policy.
    """
    return AuthorizationPolicy.from_dict(
        {
            "apiVersion": "security.istio.io/v1",
            "kind": "AuthorizationPolicy",
            "metadata": {
                "name": policy_name,
                "namespace": workload_namespace,
                "labels": workload_owner_labels(workload_namespace, workload_service_account),
            },
            "spec": {
                "selector": {
                    "matchLabels": {
                        "spark-role": role,
                    },
                },
                "action": "ALLOW",
                "rules": [
                    {
                        "from": [
                            {
                                "source": {
                                    "principals": [
                                        f"cluster.local/ns/{workload_namespace}/sa/{workload_service_account}",
                                    ],
                                },
                            },
                        ],
                    },
                ],
            },
        }
    )


def get_client_app_auth_policy(
    policy_name: str,
    app_namespace: str,
    app_name: str,
    workload_namespace: str,
    workload_service_account: str,
):
    """Get the policy that lets the workload driver reach a client application.

    Selects the client application pods (in the client application namespace) and allows the
    workload service account as the source principal.

    Args:
        policy_name (str): The name of the authorization policy.
        app_namespace (str): The namespace of the client application.
        app_name (str): The name (and service account) of the client application.
        workload_namespace (str): The namespace of the workload.
        workload_service_account (str): The service account of the workload.

    Returns:
        AuthorizationPolicy: The constructed authorization policy.
    """
    return AuthorizationPolicy.from_dict(
        {
            "apiVersion": "security.istio.io/v1",
            "kind": "AuthorizationPolicy",
            "metadata": {
                "name": policy_name,
                "namespace": app_namespace,
                "labels": client_app_policy_labels(
                    workload_namespace,
                    workload_service_account,
                    app_namespace,
                    app_name,
                ),
            },
            "spec": {
                "selector": {
                    "matchLabels": {
                        "app.kubernetes.io/name": app_name,
                    },
                },
                "action": "ALLOW",
                "rules": [
                    {
                        "from": [
                            {
                                "source": {
                                    "principals": [
                                        f"cluster.local/ns/{workload_namespace}/sa/{workload_service_account}"
                                    ],
                                },
                            },
                        ],
                    },
                ],
            },
        }
    )


def get_client_app_to_driver_auth_policy(
    policy_name: str,
    workload_namespace: str,
    workload_service_account: str,
    client_app_namespace: str,
    client_app_service_account: str,
):
    """Get the policy that lets a client application reach the workload driver.

    Selects the Spark driver pods (in the workload namespace) and allows the client
    application service account as the source principal. One such policy exists per
    (workload, client application) relation, labelled with both identities.

    Args:
        policy_name (str): The name of the authorization policy.
        workload_namespace (str): The namespace of the workload.
        workload_service_account (str): The service account of the workload.
        client_app_namespace (str): The namespace of the client application.
        client_app_service_account (str): The service account of the client application.

    Returns:
        AuthorizationPolicy: The constructed authorization policy.
    """
    return AuthorizationPolicy.from_dict(
        {
            "apiVersion": "security.istio.io/v1",
            "kind": "AuthorizationPolicy",
            "metadata": {
                "name": policy_name,
                "namespace": workload_namespace,
                "labels": client_app_policy_labels(
                    workload_namespace,
                    workload_service_account,
                    client_app_namespace,
                    client_app_service_account,
                ),
            },
            "spec": {
                "selector": {
                    "matchLabels": {
                        "spark-role": "driver",
                    },
                },
                "action": "ALLOW",
                "rules": [
                    {
                        "from": [
                            {
                                "source": {
                                    "principals": [
                                        f"cluster.local/ns/{client_app_namespace}/sa/{client_app_service_account}"
                                    ],
                                },
                            },
                        ],
                    },
                ],
            },
        }
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


def delete_integration_hub_auth_policies(
    client: Client,
    workload_namespace: str,
    workload_service_account: str,
    keep: set[tuple[str, str]] | None = None,
):
    """Delete dangling authorization policies owned by a workload service account.

    Policies are matched by label across all namespaces. Any policy whose
    (namespace, name) is present in ``keep`` is left untouched; everything else is deleted.
    This lets callers apply the desired policies in place and remove only obsolete ones
    (e.g. from a removed client application or a disabled service mesh) without tearing
    down and recreating policies that are still desired, which would momentarily interrupt
    mesh authorization for established connections.

    Args:
        client (Client): The Lightkube client instance.
        workload_namespace (str): The namespace of the workload.
        workload_service_account (str): The service account of the workload.
        keep (set[tuple[str, str]] | None): (namespace, name) pairs to preserve.
    """
    keep = keep or set()
    labels: dict[str, Any] = workload_owner_labels(workload_namespace, workload_service_account)
    try:
        policies = list(client.list(AuthorizationPolicy, namespace="*", labels=labels))
    except (ApiError, httpx2.HTTPStatusError) as e:
        logger.info(f"Api error while listing authorization policies with labels {labels}: {e}")
        return
    for policy in policies:
        name = cast(str, getattr(policy.metadata, "name"))
        namespace = cast(str, getattr(policy.metadata, "namespace"))
        if (namespace, name) in keep:
            continue
        delete_resource_if_exists(client, AuthorizationPolicy, namespace, name)


def service_account_exists(client: Client, namespace: str, name: str) -> bool:
    """Return whether the named service account still exists.

    Unknown API errors are treated as "exists" so that transient failures never cause a
    valid policy to be garbage-collected.
    """
    try:
        client.get(ServiceAccount, name=name, namespace=namespace)
        return True
    except ApiError as e:
        if e.status.code == 404:
            return False
        logger.info(f"Api error while checking service account {namespace}:{name}: {e}")
        return True
    except httpx2.HTTPStatusError as e:
        if e.response.status_code == 404:
            return False
        logger.info(f"Api error while checking service account {namespace}:{name}: {e}")
        return True


def _garbage_collect_orphaned_resources(
    client: Client,
    resource_type: type[NamespacedResource],
    existence: dict[tuple[str, str], bool],
) -> None:
    """Delete integration hub resources of ``resource_type`` whose workload SA is gone.

    Resources are matched by the ``managed-by`` label and must carry the workload-namespace
    and workload-service-account labels; those without an owning workload service account
    (e.g. the shared truststore secret) are left untouched. ``existence`` caches service
    account lookups so each account is checked at most once across resource types.
    """
    try:
        resources = list(
            client.list(
                resource_type,
                namespace="*",
                labels={MANAGED_BY_LABEL: MANAGED_BY_INTEGRATION_HUB},
            )
        )
    except (ApiError, httpx2.HTTPStatusError) as e:
        logger.info(
            f"Api error while listing {resource_type.__name__} for garbage collection: {e}"
        )
        return

    for resource in resources:
        metadata = getattr(resource, "metadata", None)
        labels = getattr(metadata, "labels", None) or {}
        workload_namespace = labels.get(WORKLOAD_NAMESPACE_LABEL)
        workload_service_account = labels.get(WORKLOAD_SERVICE_ACCOUNT_LABEL)
        if not workload_namespace or not workload_service_account:
            # No owning workload service account (e.g. the shared truststore secret); skip.
            continue
        key = (workload_namespace, workload_service_account)
        if key not in existence:
            existence[key] = service_account_exists(
                client, workload_namespace, workload_service_account
            )
        if existence[key]:
            continue
        name = cast(str, getattr(metadata, "name"))
        namespace = cast(str, getattr(metadata, "namespace"))
        logger.info(
            f"Garbage-collecting orphaned {resource_type.__name__} {name} in namespace "
            f"{namespace}: workload service account "
            f"{workload_namespace}:{workload_service_account} no longer exists"
        )
        delete_resource_if_exists(client, resource_type, namespace, name)


def garbage_collect_orphaned_resources(client: Client) -> None:
    """Delete integration hub policies and config secrets whose workload service account is gone.

    The per-service-account reconcile only cleans up resources for service accounts it
    currently observes via the watch. If a service account is deleted while its DELETE event
    is missed (e.g. during a watch resync), its authorization policies and config secret would
    leak because no later reconcile revisits a service account that no longer exists. This pass
    reconciles from actual cluster state: any managed resource whose owning workload service
    account (read from its labels) no longer exists is removed. The shared truststore secret is
    not owned by a single account and is left untouched.

    Args:
        client (Client): The Lightkube client instance.
    """
    # Share the service-account existence cache so each account is checked at most once.
    existence: dict[tuple[str, str], bool] = {}
    _garbage_collect_orphaned_resources(client, AuthorizationPolicy, existence)
    _garbage_collect_orphaned_resources(client, Secret, existence)
