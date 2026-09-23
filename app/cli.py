#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""The command-line interface (CLI) module for the Integration Hub watcher process."""

import argparse

from spark8t.literals import HUB_LABEL as SPARK8T_HUB_LABEL

from app.constants import TIMEOUT_DEFAULT_SECONDS


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for the Integration Hub watcher process."""
    parser = argparse.ArgumentParser(
        description=(
            "The command-line interface for the Integration Hub watcher process, "
            "that monitors the services accounts and creates necessary resources for them."
        )
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
    parser.add_argument(
        "-l",
        "--allowlist",
        help="The path of the file where the service account allowlist is specified.",
        type=str,
    )
    parser.add_argument(
        "-t",
        "--timeout",
        help="The timeout in seconds for the client to close the request to watch the K8s resource.",
        default=TIMEOUT_DEFAULT_SECONDS,
        type=int,
    )
    parser.add_argument(
        "-s",
        "--truststore",
        help="The path of the truststore file.",
        type=str,
    )
    parser.add_argument(
        "-n",
        "--truststore-secret-name",
        help="The name of the truststore secret.",
        type=str,
        default=f"{SPARK8T_HUB_LABEL}-truststore",
    )
    parser.add_argument(
        "-m",
        "--service-mesh-enabled",
        help="Flag indicating if the service mesh is enabled for Spark workloads.",
        action="store_true",
    )
    parser.add_argument(
        "--client-app-service-accounts",
        help="The comma-separated list of the primary service accounts for the client applications.",
        type=str,
        default="",
    )
    return parser.parse_args()
