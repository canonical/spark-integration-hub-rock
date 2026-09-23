import base64
import logging
from typing import cast

from lightkube import Client
from lightkube.resources.apps_v1 import Deployment
from lightkube.resources.core_v1 import Pod, Secret
from spark8t.utils import K8sSecretKeySerializer
from utils import (
    HUB_NAMESPACE,
    TEST_IMAGE_OCI,
    create_service_account,
    delete_service_account,
    disable_service_mesh,
    enable_service_mesh,
    get_resource,
    wait_until,
)

from app.models import AuthorizationPolicy

TEST_SERVICE_ACCOUNT = "spark-test"


logger = logging.getLogger(__name__)


def test_rock_image_loaded_to_registry(test_hub_image: str):
    """Test that the rock image is loaded into the container runtime."""
    assert test_hub_image == TEST_IMAGE_OCI


def test_integration_hub_deployment(client: Client, integration_hub_deployment: str):
    """Test that the integration hub deployment deploys correctly."""
    deployment = cast(
        Deployment, get_resource(client, Deployment, integration_hub_deployment, HUB_NAMESPACE)
    )
    assert deployment is not None

    # The rollout completed: all desired replicas are available and ready.
    assert (deployment.status.availableReplicas or 0) == 1
    assert (deployment.status.readyReplicas or 0) == 1

    # A pod exists, is Running, ready, and has not crash-looped.
    pods = list(
        client.list(Pod, namespace=HUB_NAMESPACE, labels={"app": integration_hub_deployment})
    )
    assert pods, "No pods found for the integration-hub deployment."
    running = [pod for pod in pods if pod.status.phase == "Running"]
    assert running, f"No running pods; phases={[pod.status.phase for pod in pods]}"
    for pod in running:
        statuses = pod.status.containerStatuses or []
        assert all(cs.ready for cs in statuses), f"Container not ready in {pod.metadata.name}"
        assert all(cs.restartCount == 0 for cs in statuses), (
            f"Container restarted in {pod.metadata.name}"
        )


def test_create_unmonitored_spark_service_account(client: Client):
    unmonitored_namespace = "unmonitored"
    unmonitored_username = "unmonitored"
    username = create_service_account(
        username=unmonitored_username, namespace=unmonitored_namespace
    )
    secret = cast(
        Secret,
        get_resource(
            client,
            resource=Secret,
            name=f"integrator-hub-conf-{username}",
            namespace=HUB_NAMESPACE,
        ),
    )
    assert secret is None
    delete_service_account(username=unmonitored_username, namespace=unmonitored_namespace)


def test_create_monitored_spark_service_account(client: Client, namespace: str):
    """Test creating a Spark service account."""
    username = create_service_account(username=TEST_SERVICE_ACCOUNT, namespace=namespace)
    wait_until(
        lambda: (
            get_resource(
                client,
                resource=Secret,
                name=f"integrator-hub-conf-{username}",
                namespace=HUB_NAMESPACE,
            )
            is not None
        ),
        description="Waiting for the driver authorization policy to be created.",
    )
    secret = cast(
        Secret,
        get_resource(
            client,
            resource=Secret,
            name=f"integrator-hub-conf-{username}",
            namespace=HUB_NAMESPACE,
        ),
    )
    assert secret is not None, (
        f"Integration Hub secret for service account {username} was not created."
    )
    assert secret.data is not None, (
        f"Integration Hub secret for service account {username} has no data."
    )

    secret_data = {
        K8sSecretKeySerializer().deserialize(key): base64.b64decode(value).decode("utf-8")
        for key, value in secret.data.items()
    }
    assert secret_data["spark.executor.instances"] == "2", (
        f"Unexpected spark properties: {secret_data}"
    )
    assert secret_data["spark.executorEnv.TEST_ENV"] == "foobar", (
        f"Unexpected spark properties: {secret_data}"
    )
    assert secret_data["spark.kubernetes.driver.label.test-label"] == "foobar", (
        f"Unexpected spark properties: {secret_data}"
    )
    assert secret_data["spark.kubernetes.executor.label.test-label"] == "foobar", (
        f"Unexpected spark properties: {secret_data}"
    )

    secret = cast(
        Secret,
        get_resource(
            client,
            resource=Secret,
            name="integrator-hub-conf-truststore",
            namespace=HUB_NAMESPACE,
        ),
    )
    assert secret is not None, "Integration Hub truststore secret was not created."


def test_enable_service_mesh(client: Client, namespace: str):
    """Test enabling service mesh for the integration hub deployment."""
    enable_service_mesh(client_app_service_accounts=f"{namespace}:client-sa")

    wait_until(
        lambda: (
            get_resource(
                client,
                resource=AuthorizationPolicy,
                name=f"integrator-hub-conf-{TEST_SERVICE_ACCOUNT}-driver-policy",
                namespace=namespace,
            )
            is not None
        ),
        description="Waiting for the driver authorization policy to be created.",
    )
    driver_auth_policy = get_resource(
        client,
        resource=AuthorizationPolicy,
        name=f"integrator-hub-conf-{TEST_SERVICE_ACCOUNT}-driver-policy",
        namespace=namespace,
    )
    assert driver_auth_policy is not None, (
        f"Authorization policy for Spark driver for service account {TEST_SERVICE_ACCOUNT} was not created."
    )
    assert driver_auth_policy["spec"]["selector"] == {"matchLabels": {"spark-role": "driver"}}
    assert driver_auth_policy["spec"]["action"] == "ALLOW"
    allowed_principals = [
        p
        for rule in driver_auth_policy["spec"]["rules"]
        for f in rule["from"]
        for p in f["source"]["principals"]
    ]
    assert set(allowed_principals) == {
        f"cluster.local/ns/{namespace}/sa/{TEST_SERVICE_ACCOUNT}",
        f"cluster.local/ns/{namespace}/sa/client-sa",
    }

    executor_auth_policy = get_resource(
        client,
        resource=AuthorizationPolicy,
        name=f"integrator-hub-conf-{TEST_SERVICE_ACCOUNT}-executor-policy",
        namespace=namespace,
    )
    assert executor_auth_policy is not None, (
        f"Authorization policy for Spark executor for service account {TEST_SERVICE_ACCOUNT} was not created."
    )
    assert executor_auth_policy["spec"]["selector"] == {"matchLabels": {"spark-role": "executor"}}
    assert executor_auth_policy["spec"]["action"] == "ALLOW"
    allowed_principals = [
        p
        for rule in executor_auth_policy["spec"]["rules"]
        for f in rule["from"]
        for p in f["source"]["principals"]
    ]
    assert set(allowed_principals) == {f"cluster.local/ns/{namespace}/sa/{TEST_SERVICE_ACCOUNT}"}

    client_app_policy = get_resource(
        client,
        resource=AuthorizationPolicy,
        name=f"integrator-hub-conf-{TEST_SERVICE_ACCOUNT}-{namespace}-client-sa-policy",
        namespace=namespace,
    )
    assert client_app_policy is not None, (
        f"Authorization policy for client app service account {namespace}:client-sa for service account {TEST_SERVICE_ACCOUNT} was not created."
    )
    assert client_app_policy["spec"]["selector"] == {
        "matchLabels": {"app.kubernetes.io/name": "client-sa"}
    }
    assert client_app_policy["spec"]["action"] == "ALLOW"
    allowed_principals = [
        p
        for rule in client_app_policy["spec"]["rules"]
        for f in rule["from"]
        for p in f["source"]["principals"]
    ]
    assert set(allowed_principals) == {f"cluster.local/ns/{namespace}/sa/{TEST_SERVICE_ACCOUNT}"}


def test_disable_service_mesh(client: Client, namespace: str):
    """Test disabling service mesh for the integration hub deployment."""
    disable_service_mesh()

    wait_until(
        lambda: (
            get_resource(
                client,
                resource=AuthorizationPolicy,
                name=f"integrator-hub-conf-{TEST_SERVICE_ACCOUNT}-driver-policy",
                namespace=namespace,
            )
            is None
        ),
        timeout=60,
        interval=3,
        description=f"Waiting for Spark driver authorization policy for service account {TEST_SERVICE_ACCOUNT} to be deleted",
    )
    driver_auth_policy = get_resource(
        client,
        resource=AuthorizationPolicy,
        name=f"integrator-hub-conf-{TEST_SERVICE_ACCOUNT}-driver-policy",
        namespace=namespace,
    )
    assert driver_auth_policy is None, (
        f"Authorization policy for Spark driver for service account {TEST_SERVICE_ACCOUNT} was not deleted."
    )
    executor_auth_policy = get_resource(
        client,
        resource=AuthorizationPolicy,
        name=f"integrator-hub-conf-{TEST_SERVICE_ACCOUNT}-executor-policy",
        namespace=namespace,
    )
    assert executor_auth_policy is None, (
        f"Authorization policy for Spark executor for service account {TEST_SERVICE_ACCOUNT} was not deleted."
    )
    client_app_policy = get_resource(
        client,
        resource=AuthorizationPolicy,
        name=f"integrator-hub-conf-{TEST_SERVICE_ACCOUNT}-client-ns-client-sa-policy",
        namespace=namespace,
    )
    assert client_app_policy is None, (
        f"Authorization policy for client app service account client-ns:client-sa for service account {TEST_SERVICE_ACCOUNT} was not deleted."
    )


def test_delete_monitored_service_account(client: Client, namespace: str):
    """Test deleting a Spark service account."""
    delete_service_account(username=TEST_SERVICE_ACCOUNT, namespace=namespace)
    secret = get_resource(
        client,
        resource=Secret,
        name=f"integrator-hub-conf-{TEST_SERVICE_ACCOUNT}",
        namespace=namespace,
    )
    assert secret is None, (
        f"Integration Hub secret for service account {TEST_SERVICE_ACCOUNT} was not deleted."
    )
