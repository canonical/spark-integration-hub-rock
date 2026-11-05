# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Read-only application index, independent of the driver-Pod watcher."""

import json
import logging
import mimetypes
import os
from html import escape
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from string import Template
from urllib.parse import urlsplit

from httpx2 import HTTPError
from lightkube import ApiError, Client
from lightkube.core.exceptions import ConfigError
from lightkube.generic_resource import GenericNamespacedResource

from app.ui_controller import (
    SPARK_APP_LABEL,
    SPARK_APP_NAME_ANNOTATION,
    UI_INTEGRATION_HUB_LABEL,
    HTTPRoute,
    UIConfig,
)

logger = logging.getLogger(__name__)
INDEX_TEMPLATE = Template((Path(__file__).parent / "index.html").read_text(encoding="utf-8"))
STATIC_DIR = Path(__file__).parent / "static"
STATIC_FILES = {
    path.name: path for path in STATIC_DIR.iterdir() if path.suffix in (".css", ".woff2")
}


def render_index(config: UIConfig, routes: list[GenericNamespacedResource]) -> str:
    """Render this hub's routes without claiming driver HTTP readiness."""
    rows = []
    for route in sorted(
        routes,
        key=lambda item: (
            (item.metadata.namespace or "") if item.metadata else "",
            (item.metadata.name or "") if item.metadata else "",
        ),
    ):
        assert route.metadata and route.metadata.namespace and route.metadata.labels
        app_id = route.metadata.labels.get(SPARK_APP_LABEL)
        if not app_id or route.metadata.deletionTimestamp:
            continue
        namespace = route.metadata.namespace
        app_name = (route.metadata.annotations or {}).get(SPARK_APP_NAME_ANNOTATION) or "Unknown"
        url = f"{config.external_url}/{namespace}/{app_id}/"
        rows.append(
            f'<tr><td>{escape(namespace)}</td><th scope="row">{escape(app_name)}</th>'
            f"<td><code>{escape(app_id)}</code></td>"
            f'<td class="ui-actions"><a class="p-button" href="{escape(url, quote=True)}" '
            'target="_blank" rel="noopener noreferrer" '
            f'aria-label="Open UI for {escape(app_name, quote=True)} (opens in a new tab)">'
            'Open UI <i class="p-icon--external-link">Opens in a new tab</i></a></td></tr>'
        )
    message = "No running applications" if not rows else ""
    detail = (
        "Submit a Spark application using a monitored service account, then refresh this page."
    )
    if not config.external_url or not config.gateway:
        message = "UI exposure is not configured"
        detail = "Connect ingress and gateway-metadata to enable application UI access."
    return INDEX_TEMPLATE.substitute(
        asset_base=escape(f"{config.prefix}/static", quote=True),
        refresh_url=escape(f"{config.prefix}/", quote=True),
        route_count=len(rows),
        route_rows="".join(rows),
        empty_state_hidden="" if message else "hidden",
        table_hidden="" if rows else "hidden",
        message=escape(message),
        detail=escape(detail),
    )


def _respond(handler: BaseHTTPRequestHandler, body: bytes, content_type: str) -> None:
    """Send index HTML or a locally bundled asset."""
    handler.send_response(200)
    handler.send_header("Content-Type", content_type)
    handler.send_header("Content-Length", str(len(body)))
    handler.send_header("Cache-Control", "no-store")
    handler.send_header("X-Content-Type-Options", "nosniff")
    handler.end_headers()
    handler.wfile.write(body)


def _serve_asset(handler: BaseHTTPRequestHandler, name: str) -> None:
    """Serve only packaged assets; never resolve a request as a file path."""
    asset = STATIC_FILES.get(name)
    if asset is None:
        handler.send_error(404, "Asset not found")
        return
    try:
        body = asset.read_bytes()
    except OSError:
        logger.exception("Unable to read index asset: %s", asset.name)
        handler.send_error(503, "Unable to retrieve index asset")
        return
    _respond(handler, body, mimetypes.guess_type(asset.name)[0] or "application/octet-stream")


def make_handler(config: UIConfig) -> type[BaseHTTPRequestHandler]:
    """Build a handler with read-only Kubernetes access and no shared watcher state."""

    class IndexHandler(BaseHTTPRequestHandler):
        """Serve the index through the ordinary ingress relation."""

        def do_GET(self) -> None:
            """Fetch the hub's route list on demand, failing explicitly on API errors."""
            if config.authorized_users is not None:
                identity = self.headers.get("X-Auth-Request-Email")
                allowed = config.authorized_users.split(",")
                if not identity or ("*" not in allowed and identity not in allowed):
                    logger.warning(
                        "Denied Spark UI index request: identity missing or unauthorized"
                    )
                    self.send_error(403, "User is not authorized to view Spark UIs")
                    return
            path = urlsplit(self.path).path
            for prefix in ("/static/", f"{config.prefix}/static/"):
                if path.startswith(prefix):
                    _serve_asset(self, path.removeprefix(prefix))
                    return
            if path not in ("/", config.prefix, config.prefix + "/"):
                self.send_error(404, "Application route unavailable; it may have finished.")
                return
            routes = []
            if config.external_url and config.gateway:
                try:
                    routes = list(
                        Client().list(
                            HTTPRoute,
                            namespace="*",
                            labels={UI_INTEGRATION_HUB_LABEL: config.owner},
                        )
                    )
                except (HTTPError, ConfigError, ApiError):
                    logger.exception("Failed to list Spark UI routes")
                    self.send_error(503, "Unable to retrieve application routes")
                    return
            body = render_index(config, routes).encode()
            _respond(self, body, "text/html; charset=utf-8")

        def log_message(self, fmt: str, *args: object) -> None:
            """Send HTTP request logs to Pebble."""
            logger.info(fmt, *args)

    return IndexHandler


def main() -> None:
    """Serve HTTP synchronously in this process; routing runs in another service."""
    config = UIConfig(**json.loads(Path(os.environ["SPARK_UI_CONFIG"]).read_text()))
    with HTTPServer(("0.0.0.0", 8080), make_handler(config)) as server:
        server.serve_forever()


if __name__ == "__main__":
    main()
