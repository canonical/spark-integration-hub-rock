"""Helpers for the rock image integration tests: build import, setup deploy/teardown."""

import logging
import shlex
import subprocess
import time
from pathlib import Path
from typing import Callable, TypeVar

from lightkube import ApiError, Client
from lightkube.core.resource import NamespacedResource

logger = logging.getLogger(__name__)

T = TypeVar("T")


SETUP_MANIFEST = Path.cwd() / "tests" / "integration" / "setup" / "integration-hub.yaml"
CK8S_CTR_BIN = "/snap/k8s/current/bin/ctr"
CK8S_CTR_COMMAND = f"{CK8S_CTR_BIN} --namespace k8s.io"
MICROK8S_CTR_COMMAND = "microk8s ctr"
TEST_IMAGE_OCI = "docker.io/library/spark-integration-hub:test"
HUB_DEPLOYMENT_NAME = "integration-hub"
HUB_NAMESPACE = "test-integration-hub"


def run_command(cmd: str | list[str]) -> str:
    """Run a command (string or list), returning its stdout and raising with stderr on failure."""
    args = shlex.split(cmd) if isinstance(cmd, str) else cmd
    logger.info("Running: %s", " ".join(args))
    result = subprocess.run(args, text=True, capture_output=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"Command failed ({result.returncode}): {' '.join(args)}\n{result.stderr}"
        )
    return result.stdout


def deploy_hub_setup(namespace: str = HUB_NAMESPACE) -> None:
    """Apply the setup manifest and wait for the watcher deployment to be ready."""
    run_command(f"kubectl apply -f {SETUP_MANIFEST}")
    # Force a fresh rollout: subPath ConfigMap mounts do not hot-reload, so the pod
    # must be recreated to pick up current spark-defaults.conf / allowlist contents.
    run_command(f"kubectl -n {namespace} rollout restart deployment/{HUB_DEPLOYMENT_NAME}")
    run_command(
        f"kubectl -n {namespace} rollout status deployment/{HUB_DEPLOYMENT_NAME} --timeout=180s"
    )
    logger.info("Deployed integration-hub setup in namespace %s", namespace)
    return HUB_DEPLOYMENT_NAME


def delete_hub_setup() -> None:
    """Delete all resources defined in the hub setup manifest."""
    run_command(f"kubectl delete -f {SETUP_MANIFEST} --ignore-not-found")
    logger.info("Deleted integration-hub setup")


def enable_service_mesh(client_app_service_accounts: str = "") -> None:
    """Enable service mesh for the integration hub deployment, optionally specifying client app service accounts."""
    logger.info(
        f"Enabling service mesh, client app service accounts: {client_app_service_accounts}..."
    )
    run_command(
        f"kubectl -n {HUB_NAMESPACE} set env deployment/{HUB_DEPLOYMENT_NAME} SERVICE_MESH_ENABLED=true CLIENT_APP_SERVICE_ACCOUNTS={client_app_service_accounts}"
    )
    logger.info("Service mesh enabled, performing rollout...")
    run_command(
        f"kubectl -n {HUB_NAMESPACE} rollout status deployment/{HUB_DEPLOYMENT_NAME} --timeout=180s"
    )


def disable_service_mesh() -> None:
    """Disable service mesh for the integration hub deployment."""
    logger.info("Disabling service mesh...")
    run_command(
        f"kubectl -n {HUB_NAMESPACE} set env deployment/{HUB_DEPLOYMENT_NAME} SERVICE_MESH_ENABLED-"
    )
    logger.info("Service mesh disabled, performing rollout...")
    run_command(
        f"kubectl -n {HUB_NAMESPACE} rollout status deployment/{HUB_DEPLOYMENT_NAME} --timeout=180s"
    )


def get_resource(
    client: Client, resource: type[NamespacedResource], name: str, namespace: str
) -> NamespacedResource | None:
    """Return the named resource, or None if it does not exist."""
    try:
        return client.get(res=resource, name=name, namespace=namespace)
    except ApiError as e:
        if e.status.code == 404:
            return None
        raise


def create_service_account(username: str, namespace: str = HUB_NAMESPACE) -> str:
    """Create a Spark service account in the given namespace and return its username."""
    run_command(
        f"spark-client.service-account-registry create --namespace {namespace} --username {username}"
    )
    return username


def delete_service_account(username: str, namespace: str = HUB_NAMESPACE) -> None:
    """Delete the specified Spark service account in the given namespace."""
    run_command(
        f"spark-client.service-account-registry delete --namespace {namespace} --username {username}"
    )


def wait_until(
    predicate: Callable[[], T],
    *,
    timeout: int = 120,
    interval: int = 3,
    description: str = "condition",
) -> T:
    """Poll predicate until it returns a truthy value, or raise TimeoutError."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if result := predicate():
            return result
        time.sleep(interval)
    raise TimeoutError(f"Timed out after {timeout}s waiting for {description}")
