#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""The reconciler module."""

import logging
import sys
from pathlib import Path
from typing import Any, cast

from lightkube.core.client import Client
from lightkube.resources.core_v1 import Secret, ServiceAccount
from spark8t.literals import HUB_LABEL

from app.constants import MANAGED_BY_LABEL, MANAGED_BY_SPARK8T
from app.utils import (
    ServiceAccountNames,
    ServiceAccountPatterns,
    create_secret_from_file,
    delete_integration_hub_auth_policies,
    delete_resource_if_exists,
    get_client_app_auth_policy,
    get_client_app_to_driver_auth_policy,
    get_integration_hub_secret,
    get_workload_auth_policy,
    is_allowed,
)

logger = logging.getLogger(__name__)
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] (%(threadName)s) (%(funcName)s) %(message)s",
)


def reconcile(
    client: Client,
    namespace: str,
    service_account: str,
    operation: str,
    monitored_accounts_pattern: list[ServiceAccountPatterns],
    spark_properties: dict[str, str],
    truststore_path: Path | None,
    truststore_secret_name: str,
    service_mesh_enabled: bool,
    client_app_service_accounts: list[str],
):
    """Reconcile service-account changes."""
    logger.info(f"Operation: {operation}")
    logger.info(f"Service account: {service_account} --- namespace: {namespace}")

    if not is_allowed(ServiceAccountNames(namespace, service_account), monitored_accounts_pattern):
        logger.info(f"{namespace}:{service_account} NOT monitored, skipping.")
        return

    logger.info(f"{len(spark_properties)} Spark properties detected...")

    hub_secret_name = f"{HUB_LABEL}-{service_account}"
    driver_auth_policy_name = f"{HUB_LABEL}-{service_account}-driver-policy"
    executor_auth_policy_name = f"{HUB_LABEL}-{service_account}-executor-policy"
    spark_allowed_apps = []
    for client_app_service_account in client_app_service_accounts:
        client_app_ns, client_app_sa = client_app_service_account.split(":")
        spark_allowed_apps.append((client_app_ns, client_app_sa))

    # Apply the desired resources in place so that unchanged reconciles are no-ops and
    # established mesh connections are never interrupted by a delete/recreate cycle.
    desired_policies: list[Any] = []
    if operation == "ADDED":
        logger.info(f"Applying integration hub secret: {hub_secret_name}...")
        integration_hub_secret = get_integration_hub_secret(
            secret_name=hub_secret_name,
            namespace=namespace,
            service_account=service_account,
            options=spark_properties,
        )
        client.apply(integration_hub_secret, force=True)

        if truststore_path and truststore_path.exists():
            logger.info(f"Applying secret for truststore: {truststore_secret_name}")
            truststore_secret = create_secret_from_file(
                truststore_secret_name, truststore_path, namespace
            )
            client.apply(truststore_secret, force=True)

        if service_mesh_enabled:
            logger.info("Applying authorization policies...")
            driver_auth_policy = get_workload_auth_policy(
                policy_name=driver_auth_policy_name,
                workload_namespace=namespace,
                workload_service_account=service_account,
                role="driver",
            )
            executor_auth_policy = get_workload_auth_policy(
                policy_name=executor_auth_policy_name,
                workload_namespace=namespace,
                workload_service_account=service_account,
                role="executor",
            )
            desired_policies = [driver_auth_policy, executor_auth_policy]
            # One pair of policies per (workload, client application) relation: the driver may
            # reach the client application, and the client application may reach the driver.
            for app_ns, app_name in spark_allowed_apps:
                desired_policies.append(
                    get_client_app_auth_policy(
                        policy_name=f"{HUB_LABEL}-{service_account}-{app_ns}-{app_name}-policy",
                        app_namespace=app_ns,
                        app_name=app_name,
                        workload_namespace=namespace,
                        workload_service_account=service_account,
                    )
                )
                desired_policies.append(
                    get_client_app_to_driver_auth_policy(
                        policy_name=f"{HUB_LABEL}-{service_account}-{app_ns}-{app_name}-to-driver-policy",
                        workload_namespace=namespace,
                        workload_service_account=service_account,
                        client_app_namespace=app_ns,
                        client_app_service_account=app_name,
                    )
                )
            for policy in desired_policies:
                client.apply(policy, force=True)
    else:
        logger.info(
            f"Operation: {operation}; removing resources for service account: "
            f"{namespace}:{service_account}"
        )
        delete_resource_if_exists(client, Secret, namespace, hub_secret_name)

    # Remove the truststore secret only when it is no longer needed: the truststore is gone
    # or no spark8t-managed service account remains in the namespace.
    if (
        not truststore_path
        or not truststore_path.exists()
        or len(
            list(
                client.list(
                    ServiceAccount,
                    namespace=namespace,
                    labels={MANAGED_BY_LABEL: MANAGED_BY_SPARK8T},
                )
            )
        )
        == 0
    ):
        delete_resource_if_exists(client, Secret, namespace, truststore_secret_name)

    # Delete only dangling authorization policies owned by this workload service account:
    # those matched by label (across namespaces) that are not part of the desired set applied
    # above. This cleans up obsolete policies (removed client app, disabled mesh, deleted
    # service account) without touching the ones still in use.
    desired_policy_keys = {
        (cast(str, policy.metadata.namespace), cast(str, policy.metadata.name))
        for policy in desired_policies
    }
    delete_integration_hub_auth_policies(
        client, namespace, service_account, keep=desired_policy_keys
    )
