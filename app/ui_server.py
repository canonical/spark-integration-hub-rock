# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Read-only application index, independent of the driver-Pod watcher."""

import json
import logging
import os
from html import escape
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import urlsplit

from httpx2 import HTTPError
from lightkube import ApiError, Client
from lightkube.core.exceptions import ConfigError
from lightkube.generic_resource import GenericNamespacedResource

from app.ui_controller import SPARK_APP_LABEL, UI_INTEGRATION_HUB_LABEL, HTTPRoute, UIConfig

logger = logging.getLogger(__name__)


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
        url = f"{config.external_url}/{namespace}/{app_id}/"
        rows.append(
            f"<tr><td>{escape(namespace)}</td><td>{escape(app_id)}</td>"
            f'<td><a href="{escape(url, quote=True)}" target="_blank" rel="noopener noreferrer">'
            "Open UI</a></td></tr>"
        )
    message = "No application routes found." if not rows else ""
    if not config.external_url or not config.gateway:
        message = "UI exposure disabled. Connect ingress and gateway-metadata."
    return (
        '<!doctype html><html><head><meta charset="utf-8">'
        "<title>Spark applications</title></head><body>"
        "<h1>Spark applications</h1>"
        "<p>Refresh to update.</p>"
        "<p>note: Listed routes do not guarantee UI readiness; pod startup may still return 503.</p>"
        f"<p>{message}</p><table><thead><tr><th>Namespace</th>"
        "<th>Application ID</th><th>UI</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody></table></body></html>"
    )


def make_handler(config: UIConfig) -> type[BaseHTTPRequestHandler]:
    """Build a handler with read-only Kubernetes access and no shared watcher state."""

    class IndexHandler(BaseHTTPRequestHandler):
        """Serve the index through the ordinary ingress relation."""

        def do_GET(self) -> None:
            """Fetch the hub's route list on demand, failing explicitly on API errors."""
            if urlsplit(self.path).path not in ("/", config.prefix, config.prefix + "/"):
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
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()
            self.wfile.write(body)

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
