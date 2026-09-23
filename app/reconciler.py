#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""The reconciler module."""

import logging
import sys
from pathlib import Path

from lightkube.core.client import Client
from lightkube.resources.core_v1 import Secret, ServiceAccount
from spark8t.literals import HUB_LABEL

from app.constants import MANAGED_BY_LABEL, MANAGED_BY_SPARK8T
from app.models import AuthorizationPolicy
from app.utils import (
    ServiceAccountNames,
    ServiceAccountPatterns,
    create_secret_from_file,
    delete_resource_if_exists,
    get_integration_hub_secret,
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
):
    """Reconcile service-account changes."""
    logger.info(f"Operation: {operation}")
    logger.info(f"Service account: {service_account} --- namespace: {namespace}")

    if not is_allowed(ServiceAccountNames(namespace, service_account), monitored_accounts_pattern):
        logger.info(f"{namespace}:{service_account} NOT monitored, skipping.")
        return

    logger.info(f"{len(spark_properties)} Spark properties detected...")

    hub_secret_name = f"{HUB_LABEL}-{service_account}"

    # Delete existing resources related to the service account.
    logger.info(
        f"Deleting existing resources for service account: {service_account} in namespace: {namespace}..."
    )
    delete_resource_if_exists(client, Secret, namespace, hub_secret_name)
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

    if operation != "ADDED":
        logger.info(
            f"Operation: {operation} is skipped for service account: {namespace}:{service_account}"
        )
        return

    logger.info(f"Updating integration hub secret: {hub_secret_name}...")
    integration_hub_secret = get_integration_hub_secret(
        secret_name=hub_secret_name, namespace=namespace, options=spark_properties
    )
    client.create(integration_hub_secret)

    if truststore_path and truststore_path.exists():
        logger.info(f"Updating secret for truststore: {truststore_secret_name}")
        delete_resource_if_exists(client, Secret, namespace, truststore_secret_name)
        truststore_secret = create_secret_from_file(
            truststore_secret_name, truststore_path, namespace
        )
        client.create(truststore_secret)
