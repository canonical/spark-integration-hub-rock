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
    assert_client_app_labels,
    assert_workload_labels,
    create_service_account,
    delete_service_account,
    disable_service_mesh,
    enable_service_mesh,
    get_resource,
    restart_hub_watcher,
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
    # The base driver policy grants only the workload's own service account; client application
    # access is granted by a separate per-relation policy asserted below.
    assert set(allowed_principals) == {f"cluster.local/ns/{namespace}/sa/{TEST_SERVICE_ACCOUNT}"}
    assert_workload_labels(driver_auth_policy, namespace, TEST_SERVICE_ACCOUNT)

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
    assert_workload_labels(executor_auth_policy, namespace, TEST_SERVICE_ACCOUNT)

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
    assert_client_app_labels(
        client_app_policy, namespace, TEST_SERVICE_ACCOUNT, namespace, "client-sa"
    )

    # The reverse direction: a dedicated per-relation policy lets the client application reach
    # the Spark driver, instead of merging client principals into the base driver policy.
    client_app_to_driver_policy = get_resource(
        client,
        resource=AuthorizationPolicy,
        name=f"integrator-hub-conf-{TEST_SERVICE_ACCOUNT}-{namespace}-client-sa-to-driver-policy",
        namespace=namespace,
    )
    assert client_app_to_driver_policy is not None, (
        f"Authorization policy letting client app {namespace}:client-sa reach the driver for "
        f"service account {TEST_SERVICE_ACCOUNT} was not created."
    )
    assert client_app_to_driver_policy["spec"]["selector"] == {
        "matchLabels": {"spark-role": "driver"}
    }
    assert client_app_to_driver_policy["spec"]["action"] == "ALLOW"
    allowed_principals = [
        p
        for rule in client_app_to_driver_policy["spec"]["rules"]
        for f in rule["from"]
        for p in f["source"]["principals"]
    ]
    assert set(allowed_principals) == {f"cluster.local/ns/{namespace}/sa/client-sa"}
    assert_client_app_labels(
        client_app_to_driver_policy, namespace, TEST_SERVICE_ACCOUNT, namespace, "client-sa"
    )


def test_authorization_policies_not_recreated_on_reconcile(client: Client, namespace: str):
    """A reconcile with unchanged desired state must not tear down and recreate policies.

    Recreating an AuthorizationPolicy momentarily removes it from the mesh and could interrupt
    established connections (e.g. a long-running Kyuubi session). The reconciler must apply
    policies in place, so their identity (uid and creationTimestamp) must remain stable across
    a reconcile that does not change the desired state.
    """
    policy_names = [
        f"integrator-hub-conf-{TEST_SERVICE_ACCOUNT}-driver-policy",
        f"integrator-hub-conf-{TEST_SERVICE_ACCOUNT}-executor-policy",
        f"integrator-hub-conf-{TEST_SERVICE_ACCOUNT}-{namespace}-client-sa-policy",
        f"integrator-hub-conf-{TEST_SERVICE_ACCOUNT}-{namespace}-client-sa-to-driver-policy",
    ]

    identities_before = {}
    for name in policy_names:
        policy = get_resource(client, resource=AuthorizationPolicy, name=name, namespace=namespace)
        assert policy is not None, f"Authorization policy {name} not found before reconcile."
        identities_before[name] = (
            policy.metadata.uid,
            policy.metadata.creationTimestamp,
        )

    # Force the watcher to re-list and reconcile the existing service account without changing
    # the desired state. restart_hub_watcher blocks until the rollout completes and the new
    # watcher pod is ready.
    restart_hub_watcher()

    for name in policy_names:
        policy = get_resource(client, resource=AuthorizationPolicy, name=name, namespace=namespace)
        assert policy is not None, (
            f"Authorization policy {name} disappeared during reconcile; it must be applied in place."
        )
        assert (policy.metadata.uid, policy.metadata.creationTimestamp) == identities_before[
            name
        ], (
            f"Authorization policy {name} was recreated during reconcile "
            "(uid/creationTimestamp changed); this would interrupt established mesh connections."
        )


def test_remove_client_application_relation(client: Client, namespace: str):
    """Test that removing a single client application deletes only its authorization policy."""
    # Add a second client application alongside the existing one.
    enable_service_mesh(
        client_app_service_accounts=f"{namespace}:client-sa,{namespace}:client-sa-2"
    )
    wait_until(
        lambda: (
            get_resource(
                client,
                resource=AuthorizationPolicy,
                name=f"integrator-hub-conf-{TEST_SERVICE_ACCOUNT}-{namespace}-client-sa-2-policy",
                namespace=namespace,
            )
            is not None
        ),
        description="Waiting for the second client application authorization policy to be created.",
    )

    # Remove the second client application, keeping the first.
    enable_service_mesh(client_app_service_accounts=f"{namespace}:client-sa")
    wait_until(
        lambda: (
            get_resource(
                client,
                resource=AuthorizationPolicy,
                name=f"integrator-hub-conf-{TEST_SERVICE_ACCOUNT}-{namespace}-client-sa-2-policy",
                namespace=namespace,
            )
            is None
        ),
        timeout=60,
        interval=3,
        description="Waiting for the removed client application authorization policy to be deleted.",
    )

    removed_policy = get_resource(
        client,
        resource=AuthorizationPolicy,
        name=f"integrator-hub-conf-{TEST_SERVICE_ACCOUNT}-{namespace}-client-sa-2-policy",
        namespace=namespace,
    )
    assert removed_policy is None, (
        "Authorization policy for the removed client application client-sa-2 was not deleted."
    )
    removed_to_driver_policy = get_resource(
        client,
        resource=AuthorizationPolicy,
        name=f"integrator-hub-conf-{TEST_SERVICE_ACCOUNT}-{namespace}-client-sa-2-to-driver-policy",
        namespace=namespace,
    )
    assert removed_to_driver_policy is None, (
        "The client-app-to-driver policy for the removed client application client-sa-2 was not deleted."
    )

    retained_policy = get_resource(
        client,
        resource=AuthorizationPolicy,
        name=f"integrator-hub-conf-{TEST_SERVICE_ACCOUNT}-{namespace}-client-sa-policy",
        namespace=namespace,
    )
    assert retained_policy is not None, (
        "Authorization policy for the retained client application client-sa was unexpectedly deleted."
    )
    retained_to_driver_policy = get_resource(
        client,
        resource=AuthorizationPolicy,
        name=f"integrator-hub-conf-{TEST_SERVICE_ACCOUNT}-{namespace}-client-sa-to-driver-policy",
        namespace=namespace,
    )
    assert retained_to_driver_policy is not None, (
        "The client-app-to-driver policy for the retained client application client-sa was "
        "unexpectedly deleted."
    )

    # Driver and executor policies must remain intact.
    for role in ("driver", "executor"):
        workload_policy = get_resource(
            client,
            resource=AuthorizationPolicy,
            name=f"integrator-hub-conf-{TEST_SERVICE_ACCOUNT}-{role}-policy",
            namespace=namespace,
        )
        assert workload_policy is not None, (
            f"Authorization policy for Spark {role} was unexpectedly deleted."
        )


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
        name=f"integrator-hub-conf-{TEST_SERVICE_ACCOUNT}-{namespace}-client-sa-policy",
        namespace=namespace,
    )
    assert client_app_policy is None, (
        f"Authorization policy for client app service account {namespace}:client-sa for service account {TEST_SERVICE_ACCOUNT} was not deleted."
    )
    client_app_to_driver_policy = get_resource(
        client,
        resource=AuthorizationPolicy,
        name=f"integrator-hub-conf-{TEST_SERVICE_ACCOUNT}-{namespace}-client-sa-to-driver-policy",
        namespace=namespace,
    )
    assert client_app_to_driver_policy is None, (
        f"Client-app-to-driver policy for {namespace}:client-sa for service account "
        f"{TEST_SERVICE_ACCOUNT} was not deleted."
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
