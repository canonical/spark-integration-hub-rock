#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Routine that updates secrets for Spark service accounts."""

import logging
import sys
from pathlib import Path
from typing import cast

from lightkube.core.client import Client
from lightkube.resources.core_v1 import ServiceAccount
from spark8t.utils import K8sSecretKeySerializer

from app.cli import parse_args
from app.constants import MANAGED_BY_LABEL, MANAGED_BY_SPARK8T
from app.reconciler import reconcile
from app.utils import (
    build_patterns,
    get_allowlist,
    read_configuration_file,
)

logger = logging.getLogger(__name__)
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] (%(threadName)s) (%(funcName)s) %(message)s",
)


def main() -> None:
    """Run the service account watcher process."""
    args = parse_args()
    logger.info("Starting service account watcher process...")
    client = Client(field_manager=args.app_name)  # type: ignore
    monitored_accounts_pattern = build_patterns(get_allowlist(Path(args.allowlist)))
    spark_properties = {
        K8sSecretKeySerializer().serialize(key): value
        for key, value in read_configuration_file(args.config).items()
    }
    truststore_path = Path(args.truststore) if args.truststore else None

    for operation, sa in client.watch(
        ServiceAccount,
        namespace="*",
        labels={MANAGED_BY_LABEL: MANAGED_BY_SPARK8T},
        # This timeout is needed for the client to not hang up indefinitely when the K8s server
        # stops responding to the watch request due to inactivity for long period of time.
        # https://github.com/canonical/spark-k8s-bundle/issues/72
        server_timeout=args.timeout,
    ):
        service_account = cast(str, getattr(sa.metadata, "name"))
        namespace = cast(str, getattr(sa.metadata, "namespace"))

        reconcile(
            client=client,
            namespace=namespace,
            service_account=service_account,
            operation=operation,
            monitored_accounts_pattern=monitored_accounts_pattern,
            spark_properties=spark_properties,
            truststore_path=truststore_path,
            truststore_secret_name=args.truststore_secret_name,
        )


if __name__ == "__main__":
    main()
