# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

from dataclasses import replace
from http.client import HTTPConnection
from http.server import HTTPServer
from threading import Thread
from unittest.mock import patch

import pytest
from lightkube.models.meta_v1 import ObjectMeta

from app.ui_controller import SPARK_APP_LABEL, SPARK_APP_NAME_ANNOTATION, HTTPRoute, UIConfig
from app.ui_server import STATIC_DIR, STATIC_FILES, make_handler, render_index


@pytest.fixture
def config():
    return UIConfig("spark-hub", "https://example.test/spark-hub", {"name": "gateway"})


@pytest.fixture
def route():
    return HTTPRoute(
        metadata=ObjectMeta(
            name="spark-ui-app",
            namespace="spark",
            labels={SPARK_APP_LABEL: "app-123"},
            annotations={
                SPARK_APP_NAME_ANNOTATION: "Spark <Pi>",
            },
        )
    )


@pytest.fixture
def http_get(config):
    with (
        patch("app.ui_server.Client") as client,
        HTTPServer(("127.0.0.1", 0), make_handler(config)) as server,
    ):
        client.return_value.list.return_value = []
        thread = Thread(target=server.serve_forever)
        thread.start()

        def get(path, headers=None):
            connection = HTTPConnection(*server.server_address, timeout=5)
            try:
                connection.request("GET", path, headers=headers or {})
                response = connection.getresponse()
                return response.status, dict(response.getheaders()), response.read()
            finally:
                connection.close()

        try:
            yield get
        finally:
            server.shutdown()
            thread.join()


def test_renderer(config, route):
    html = render_index(config, [route])
    assert "Spark &lt;Pi&gt;" in html
    assert 'href="/spark-hub/static/vanilla-4.30.0.min.css"' in html
    assert 'target="_blank" rel="noopener noreferrer"' in html
    assert "1 active application routes" in html
    assert "Skip to content" in html
    assert '<th scope="col"><span class="u-off-screen">UI link</span></th>' in html
    assert "${" not in html
    assert '<div class="p-card ui-empty" hidden>' in html
    assert 'aria-label="Application list" >' in html


def test_empty_states(config):
    html = render_index(config, [])
    assert "No running applications" in html
    assert '<div class="p-card ui-empty" >' in html
    assert 'aria-label="Application list" hidden>' in html
    assert "UI exposure is not configured" in render_index(replace(config, external_url=None), [])


def test_index_response(http_get):
    status, headers, body = http_get("/spark-hub/")
    assert status == 200
    assert headers["Content-Type"] == "text/html; charset=utf-8"
    assert headers["Cache-Control"] == "no-store"
    assert int(headers["Content-Length"]) == len(body)
    assert b"No running applications" in body


def test_renderer_escapes_link(config, route):
    route.metadata.labels[SPARK_APP_LABEL] = 'app-123"><script>alert(1)</script>'
    html = render_index(config, [route])
    assert "<script>" not in html
    assert "&quot;&gt;&lt;script&gt;" in html


@pytest.mark.parametrize("prefix", ["/static/", "/spark-hub/static/"])
@pytest.mark.parametrize("name", sorted(STATIC_FILES))
def test_assets(http_get, prefix, name):
    status, headers, body = http_get(prefix + name)
    assert status == 200
    assert body == STATIC_FILES[name].read_bytes()
    assert headers["X-Content-Type-Options"] == "nosniff"
    assert headers["Content-Type"].startswith(
        "text/css" if name.endswith(".css") else ("font/" if name.endswith(".woff2") else "image/")
    )


@pytest.mark.parametrize(
    "path",
    [
        "/static/../ui_server.py",
        "/static/%2e%2e/ui_server.py",
        "/static/vanilla-LICENSE",
        "/static/missing.css",
        "/static/vanilla-asset-1.png",
        "/static/vanilla-asset-0.jpg",
        "/static/index.html",
        "/index.html",
    ],
)
def test_unknown_assets(http_get, path):
    assert http_get(path)[0] == 404


@pytest.mark.parametrize(
    "config", [UIConfig("hub", None, None, authorized_users="alice@example.test")]
)
def test_assets_preserve_authorization(http_get):
    assert http_get("/static/index.css")[0] == 403
    assert http_get("/static/index.css", {"X-Auth-Request-Email": "bob@example.test"})[0] == 403
    assert http_get("/static/index.css", {"X-Auth-Request-Email": "alice@example.test"})[0] == 200


def test_missing_asset(http_get):
    with patch("pathlib.Path.read_bytes", side_effect=OSError("missing")):
        assert http_get("/static/index.css")[0] == 503


def test_stylesheet_offline():
    css = (STATIC_DIR / "vanilla-4.30.0.min.css").read_text()
    assert "https://assets.ubuntu.com" not in css
    assert all(name in css for name in STATIC_FILES if name.startswith("vanilla-asset"))
    assert ".png" not in css
    assert ".jpg" not in css
    assert not any(path.suffix in (".png", ".jpg") for path in STATIC_DIR.iterdir())
    assert len(list(STATIC_DIR.glob("*.woff2"))) == 3
    assert "data:image/svg+xml" in css


def test_placeholder_like_route_values(config, route):
    route.metadata.annotations[SPARK_APP_NAME_ANNOTATION] = "${route_rows} $100"
    assert "${route_rows} $100" in render_index(config, [route])
