# spark-integration-hub-rock

Charmed Spark is a set of Canonical supported artifacts (including charms, rocks and SNAPs) that makes operating Spark workloads on Kubernetes seamless, secure and production-ready.

This repository contains the packing and metadata for the Spark Integrator Hub Rock image. 
It contains the business logic and component to update and to provide the Spark configuration options to the final user in an automatic way.

## Build

Building requires to have installed `rockcraft`. In Ubuntu, you can install `rockcraft` with

```bash 
snap install rockcraft --classic
```

Then, to build the Spark Integration rock image, simply use

```bash
rockcraft pack
```

This will create a `.rock` file that is an OCI compliant image. To upload the file to your local Docker registry use skopeo that is bundled in the `rockcraft` snap:

```bash
sudo rockcraft.skopeo --insecure-policy copy oci-archive:<file> docker-daemon:<image>:<tag>
```

## Live Spark UI POC

`app.spark_ui` is a driver-Pod watcher and route-controller process.
`app.spark_ui_index` is a separate read-only HTTP-index process on port 8080.
Their independent Pebble services, `spark-ui` and `spark-ui-index`, are configured
and enabled by the matching Integration Hub charm, and disabled in the standalone
rock until the charm supplies their settings.

Configuration is supplied through:

- `SPARK_UI_CONFIG`: a JSON file containing `owner` (the hub ownership label),
  `external_url` (including the ingress prefix), `gateway` (namespace, gateway_name,
  deployment_name, service_account).
- `SA_ALLOWLIST`: the existing service-account allowlist file.

Setting `external_url` and `gateway` to `null` disables exposure and removes this
hub's managed routes. The index then displays an exposure-disabled message.

The process watches only Pods labelled `spark-role=driver`. Exposure is scoped
by the existing allowlist of spark8t-managed service accounts, not a separate
opt-in label. For running
drivers it looks up Services in the same namespace by `spark-app-selector`,
verifies Pod ownership and selectors, and reads the named `spark-ui` port.
It then creates Gateway API v1 HTTPRoutes. Routes are owned by the driver Service and labelled
`spark.canonical.com/ui-integration-hub=<owner>` for isolated cleanup. Initial reconciliation
and reconciliation after watch events or a 30-second idle wait handle missed events,
late or changed Services and account changes. Completed,
failed, and deleting drivers are excluded and their exposure is removed.

The process has one background Pod-watch thread and a main reconciliation loop.
It logs application URLs as
`<external_url>/<namespace>/<spark-app-selector>/`, where `external_url` includes
the ingress-allocated `/<model>-<hub-app>` prefix. Job UI traffic goes directly
from the Gateway to driver Services, not through the index.

The independent index service lists this hub's labelled HTTPRoutes on each page
request and renders namespace, application ID, and UI link. It has no Pod watch
or resource-writing logic and does not share mutable state with the watcher.
Its main process blocks in `HTTPServer.serve_forever()`, handling requests
sequentially without spawning request-handler threads.
The base ingress URL reaches its `/` handler after prefix stripping; unknown
application paths return 404. Kubernetes discovery errors are logged and return
503. Application links indicate configured routes, not HTTP readiness; they may
return transient 503s during startup. No proxy runs in this image.

AuthorizationPolicies remain the responsibility of the original `integration-hub`
service. The charm adds the gateway's `namespace:service_account` identity to
`CLIENT_APP_SERVICE_ACCOUNTS` alongside existing client identities. When mesh is
enabled, that watcher uses its existing policy builder to allow the gateway to
reach driver Pods. Those policies permit all driver ports in the namespace, not
only the UI port, and are owned by workload service accounts rather than drivers.
The new route watcher does not write or delete AuthorizationPolicies.

The current POC assumes standard Istio ingress listeners (`http-80` and
`https-443`). The existing mesh-policy builder assumes `cluster.local`.
Neither service authenticates users or filters the index by tenant:
configure ingress authentication and use only a
trusted POC environment. It supports cluster-mode drivers in monitored accounts,
not client-mode drivers. Existing applications are discoverable, but need the
Hub's `spark.ui.proxyRedirectUri` configuration for correct external redirects.
Avoid overlapping account scopes between hubs; the POC does not arbitrate them.