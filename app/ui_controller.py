# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Watch monitored Spark drivers and reconcile direct UI routes."""

import hashlib
import json
import logging
import os
import time
from dataclasses import dataclass
from functools import cached_property
from ipaddress import ip_address
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from httpx2 import HTTPStatusError
from lightkube import Client
from lightkube.core.exceptions import ApiError
from lightkube.generic_resource import GenericNamespacedResource, create_namespaced_resource
from lightkube.resources.core_v1 import Pod, Service, ServiceAccount

from app.constants import MANAGED_BY_INTEGRATION_HUB, MANAGED_BY_LABEL, MANAGED_BY_SPARK8T
from app.utils import (
    ServiceAccountNames,
    ServiceAccountPatterns,
    build_patterns,
    get_allowlist,
    is_allowed,
)

logger = logging.getLogger(__name__)
UI_INTEGRATION_HUB_LABEL = "spark.canonical.com/ui-integration-hub"
SPARK_APP_LABEL = "spark-app-selector"
SPARK_APP_NAME_LABEL = "spark-app-name"
SPARK_APP_NAME_ANNOTATION = "spark.canonical.com/spark-app-name"
HTTPRoute = create_namespaced_resource(
    "gateway.networking.k8s.io", "v1", "HTTPRoute", "httproutes"
)


@dataclass(frozen=True)
class UIConfig:
    """Route configuration supplied by the charm's ingress relations."""

    owner: str
    external_url: str | None
    gateway: dict[str, str] | None
    authorized_users: str | None = None

    @property
    def prefix(self) -> str:
        """Return the path allocated by the ordinary ingress relation."""
        return urlsplit(self.external_url or "").path.rstrip("/")


def route_resource(
    config: UIConfig, pod: Pod, service: Service, port: int
) -> GenericNamespacedResource:
    """Build an application route owned by its Spark-created driver Service."""
    assert pod.metadata and pod.metadata.labels and service.metadata and config.gateway
    assert service.metadata.uid and service.metadata.namespace and service.metadata.name
    app_id = pod.metadata.labels[SPARK_APP_LABEL]
    app_name = pod.metadata.labels.get(SPARK_APP_NAME_LABEL)
    prefix = f"{config.prefix}/{service.metadata.namespace}/{app_id}"
    suffix = hashlib.sha256(service.metadata.uid.encode()).hexdigest()[:16]
    parsed = urlsplit(config.external_url or "")
    manifest: dict[str, Any] = {
        "apiVersion": "gateway.networking.k8s.io/v1",
        "kind": "HTTPRoute",
        "metadata": {
            "name": f"{app_id}-{config.owner}-{suffix}",
            "namespace": service.metadata.namespace,
            "labels": {
                MANAGED_BY_LABEL: MANAGED_BY_INTEGRATION_HUB,
                UI_INTEGRATION_HUB_LABEL: config.owner,
                SPARK_APP_LABEL: app_id,
            },
            "annotations": ({SPARK_APP_NAME_ANNOTATION: app_name} if app_name else {}),
            # important -- set Service as the owner, such that this gets auto cleaned up when service goes down
            "ownerReferences": [
                {
                    "apiVersion": "v1",
                    "kind": "Service",
                    "name": service.metadata.name,
                    "uid": service.metadata.uid,
                }
            ],
        },
        "spec": {
            "parentRefs": [
                {
                    "group": "gateway.networking.k8s.io",
                    "kind": "Gateway",
                    "name": config.gateway["gateway_name"],
                    "namespace": config.gateway["namespace"],
                    "sectionName": "https-443" if parsed.scheme == "https" else "http-80",
                }
            ],
            "rules": [
                {
                    "matches": [{"path": {"type": "PathPrefix", "value": prefix}}],
                    "filters": [
                        {
                            "type": "URLRewrite",
                            "urlRewrite": {
                                "path": {
                                    "type": "ReplacePrefixMatch",
                                    "replacePrefixMatch": "/",
                                }
                            },
                        },
                        {
                            "type": "RequestHeaderModifier",
                            "requestHeaderModifier": {
                                "set": [{"name": "X-Forwarded-Context", "value": prefix}]
                            },
                        },
                    ],
                    "backendRefs": [{"name": service.metadata.name, "port": port}],
                }
            ],
        },
    }
    # Gateway API forbids IP literals in hostnames, including development LB addresses.
    assert parsed.hostname
    try:
        ip_address(parsed.hostname)
    except ValueError:
        manifest["spec"]["hostnames"] = [parsed.hostname]
    return HTTPRoute.from_dict(manifest)


class UIController:
    """Manage only HTTPRoutes; the existing account watcher manages mesh policies."""

    def __init__(self, config: UIConfig, allowlist: Path):
        self.config = config
        self.allowlist = allowlist

    @cached_property
    def client(self) -> Client:
        """Create a Kubernetes client scoped to this hub's field manager."""
        return Client(field_manager=f"spark-ui-{self.config.owner}")

    def reconcile(self) -> None:
        """Rebuild route state after Pod events, restarts, and periodic retries."""
        desired: set[tuple[str, str]] = set()
        if self.config.external_url and self.config.gateway:
            patterns = build_patterns(get_allowlist(self.allowlist))
            accounts = {
                (sa.metadata.namespace, sa.metadata.name)
                for sa in self.client.list(
                    ServiceAccount, namespace="*", labels={MANAGED_BY_LABEL: MANAGED_BY_SPARK8T}
                )
                if sa.metadata
            }
            for pod in self.client.list(Pod, namespace="*", labels={"spark-role": "driver"}):
                if self._eligible(pod, accounts, patterns):
                    if key := self._reconcile_driver(pod):
                        desired.add(key)
        self._prune(desired)

    def _reconcile_driver(self, pod: Pod) -> tuple[str, str] | None:
        """Resolve the actual driver Service instead of inferring its name."""
        assert pod.metadata and pod.metadata.labels
        services = self.client.list(
            Service,
            namespace=pod.metadata.namespace,
            labels={SPARK_APP_LABEL: pod.metadata.labels[SPARK_APP_LABEL]},
        )
        for service in services:
            if port := self._ui_port(pod, service):
                route = route_resource(self.config, pod, service, port)
                assert route.metadata and route.metadata.namespace and route.metadata.name
                self.client.apply(route, force=True)
                logger.info(
                    "Reconciled Spark UI %s/%s: %s/%s/%s/",
                    route.metadata.namespace,
                    route.metadata.name,
                    self.config.external_url,
                    pod.metadata.namespace,
                    pod.metadata.labels[SPARK_APP_LABEL],
                )
                return route.metadata.namespace, route.metadata.name
        logger.info(
            "Waiting for driver Service for %s/%s", pod.metadata.namespace, pod.metadata.name
        )
        return None

    @staticmethod
    def _eligible(
        pod: Pod,
        accounts: set[tuple[str | None, str | None]],
        patterns: list[ServiceAccountPatterns],
    ) -> bool:
        """Whether spark driver is eligible for exposing its UI."""
        if not pod.metadata or not pod.spec or not pod.status:
            return False
        namespace = pod.metadata.namespace
        assert namespace
        account = pod.spec.serviceAccountName or "default"
        return bool(
            pod.status.phase == "Running"
            and not pod.metadata.deletionTimestamp
            and (pod.metadata.labels or {}).get(SPARK_APP_LABEL)
            and (namespace, account) in accounts
            and is_allowed(ServiceAccountNames(namespace, account), patterns)
        )

    @staticmethod
    def _ui_port(pod: Pod, service: Service) -> int | None:
        """Find the named UI port on a Service owned by and selecting this driver."""
        assert pod.metadata
        if not service.metadata or not service.spec or service.metadata.deletionTimestamp:
            return None
        if service.metadata.namespace != pod.metadata.namespace:
            return None
        if not any(
            owner.kind == "Pod" and owner.uid == pod.metadata.uid
            for owner in service.metadata.ownerReferences or []
        ):
            return None
        selector = service.spec.selector or {}
        if not selector or not all(
            (pod.metadata.labels or {}).get(key) == value for key, value in selector.items()
        ):
            return None
        return next((p.port for p in service.spec.ports or [] if p.name == "spark-ui"), None)

    def _prune(self, desired: set[tuple[str, str]]) -> None:
        """Remove only this hub's obsolete routes, never account-watcher policies."""
        try:
            routes = list(
                self.client.list(
                    HTTPRoute, namespace="*", labels={UI_INTEGRATION_HUB_LABEL: self.config.owner}
                )
            )
        except HTTPStatusError as exc:
            # A missing CRD yields a text/plain 404, which lightkube does not wrap in ApiError.
            if exc.response.status_code == 404 and not self.config.external_url:
                logger.debug("HTTPRoute CRD is not installed; exposure is disabled")
                return
            raise
        for route in routes:
            assert route.metadata and route.metadata.namespace and route.metadata.name
            key = (route.metadata.namespace, route.metadata.name)
            if key not in desired:
                logger.info("Removing HTTPRoute %s/%s", *key)
                try:
                    self.client.delete(HTTPRoute, key[1], namespace=key[0])
                except ApiError as exc:
                    if exc.status.code != 404:
                        raise
                    logger.debug("Route already garbage-collected: %s", key)


def main() -> None:
    """Reconcile routes on every driver Pod event; Pebble restarts the service on failure."""
    logging.basicConfig(level=logging.INFO)
    config = UIConfig(**json.loads(Path(os.environ["SPARK_UI_CONFIG"]).read_text()))
    controller = UIController(config, Path(os.environ["SA_ALLOWLIST"]))
    if not (config.external_url and config.gateway):
        logger.info("Spark UI routing disabled; waiting for ingress configuration")
        while True:
            controller.reconcile()
            time.sleep(30)
    while True:
        # The watch replays existing Pods first, then streams changes until the server timeout.
        for _event in controller.client.watch(
            Pod, namespace="*", labels={"spark-role": "driver"}, server_timeout=30
        ):
            controller.reconcile()
        controller.reconcile()


if __name__ == "__main__":
    main()
