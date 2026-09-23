import logging
import shutil
import uuid
from pathlib import Path
from platform import machine
from typing import Generator

import pytest
from lightkube import Client
from utils import (
    CK8S_CTR_BIN,
    CK8S_CTR_COMMAND,
    HUB_NAMESPACE,
    MICROK8S_CTR_COMMAND,
    TEST_IMAGE_OCI,
    delete_hub_setup,
    deploy_hub_setup,
    run_command,
)

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def client() -> Client:
    """Lightkube client for asserting cluster state."""
    return Client()


@pytest.fixture(scope="module")
def k8s_runtime() -> str:
    """Return the local Kubernetes runtime: 'canonical-k8s' or 'microk8s'."""
    if Path(CK8S_CTR_BIN).exists():
        return "k8s"
    if shutil.which("microk8s"):
        return "microk8s"
    raise RuntimeError("No supported Kubernetes runtime found (microk8s or canonical-k8s).")


@pytest.fixture(scope="module")
def platform() -> str:
    """Fixture to provide the platform architecture for testing."""
    platforms = {
        "x86_64": "amd64",
        "aarch64": "arm64",
    }
    return platforms.get(machine(), "amd64")


@pytest.fixture(scope="module")
def test_hub_image(k8s_runtime: str, platform: str) -> str:
    """Return the test hub image name."""
    ctr_bin = CK8S_CTR_COMMAND if k8s_runtime == "k8s" else MICROK8S_CTR_COMMAND

    if not (path := next(iter(Path.cwd().glob(f"*_{platform}.rock")), None)):
        raise FileNotFoundError("Could not find packed integration hub rock.")

    logger.info("Loading test hub image into container runtime...")
    run_command(
        f"rockcraft.skopeo copy --insecure-policy  oci-archive:{path}  docker-archive:spark-integration-hub.tar:{TEST_IMAGE_OCI}"
    )
    run_command(f"sudo {ctr_bin} image import spark-integration-hub.tar")
    run_command("rm spark-integration-hub.tar")

    assert TEST_IMAGE_OCI in run_command(f"sudo {ctr_bin} image list")
    return TEST_IMAGE_OCI


@pytest.fixture(scope="module")
def integration_hub_deployment(test_hub_image: str, namespace: str) -> Generator:
    """Deploy the integration hub setup for the module, tearing it down afterwards."""
    deployment_name = deploy_hub_setup()
    yield deployment_name
    delete_hub_setup()


@pytest.fixture(scope="module")
def namespace() -> Generator:
    yield HUB_NAMESPACE


@pytest.fixture(scope="module")
def client_app_service_account(namespace: str) -> Generator:
    username = f"client-{uuid.uuid4().hex[:8]}"
    run_command(
        f"spark-client.service-account-registry create --namespace {namespace} --username {username}"
    )
    yield username
    run_command(
        f"spark-client.service-account-registry delete --namespace {namespace} --username {username}"
    )
