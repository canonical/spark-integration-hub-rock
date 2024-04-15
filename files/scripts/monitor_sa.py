#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""Routine that updates secrets for Spark service accounts."""

import argparse
import logging
import os
import sys
from typing import Dict, Optional

from lightkube.core.client import Client
from lightkube.core.exceptions import ApiError
from lightkube.resources.core_v1 import Secret, ServiceAccount

from spark8t.utils import PercentEncodingSerializer
from spark8t.literals import HUB_LABEL

logger = logging.getLogger(__name__)
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] (%(threadName)s) (%(funcName)s) %(message)s",
)


def read_configuration_file(file_path: str) -> Optional[Dict[str, str]]:
    """Read spark configuration file."""
    if not os.path.exists(file_path):
        return None
    kubernetes_key_serializer = PercentEncodingSerializer("_")
    with open(file_path, "r") as f:
        lines = f.readlines()
        confs = {}
        for line in lines:
            if "=" in line:
                key_value = line.split("=")
                # encode key
                confs[kubernetes_key_serializer.serialize(key_value[0])] = key_value[1]
        return confs


if __name__ == "__main__":
    logger.info("Start process")
    parser = argparse.ArgumentParser(
        description="Handler for running a Python scripts that pushes "
    )
    parser.add_argument(
        "-a",
        "--app-name",
        help="The name of the application",
        required=True,
        type=str,
        default="",
    )
    parser.add_argument(
        "-c",
        "--config",
        help="The configuration path.",
        type=str,
    )
    args = parser.parse_args()
    logger.info("Start process that update service account secrets.")
    client = Client(field_manager=args.app_name)  # type: ignore
    label_selector = {"app.kubernetes.io/managed-by": "spark8t"}

    for op, sa in client.watch(ServiceAccount, namespace="*", labels=label_selector):
        sa_name = sa.metadata.name
        namespace = sa.metadata.namespace
        logger.info(f"Operation: {op}")
        logger.info(f"Service account: {sa_name} --- namespace: {namespace}")
        # skip in case of deletion or operation that do not need secret update.
        logger.info(f"Config file: {args.config}")
        options = read_configuration_file(args.config)
        if options:
            logger.info(f"Number of options: {len(options)}")
        else:
            logger.info("Empty configuration. No secret to update.")

        secret_name = f"{HUB_LABEL}-{sa_name}"
        # if secret is already there, delete it.
        try:
            s = client.get(Secret, name=secret_name, namespace=namespace)
            print(f"retrieved secrets: {s}")
            client.delete(Secret, name=secret_name, namespace=namespace)
        except ApiError as e:
            logger.info(f"Api error: {e}")

        if op != "ADDED":
            logger.info(f"Operation: {op} is skipped!")
            continue

        logger.info(f"Updating secret: {secret_name}")
        s = Secret.from_dict(
            {
                "apiVersion": "v1",
                "kind": "Secret",
                "metadata": {
                    "name": secret_name,
                    "namespace": namespace,
                    "labels": {"app.kubernetes.io/managed-by": "integration-hub"}
                },
                "stringData": options if options else {},
            }
        )
        # Create secret
        client.create(s)
        logger.info("--------------------------------------------------------")
