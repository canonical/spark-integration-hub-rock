#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""The constants module."""

TIMEOUT_DEFAULT_SECONDS = 30
MANAGED_BY_LABEL = "app.kubernetes.io/managed-by"
MANAGED_BY_SPARK8T = "spark8t"
MANAGED_BY_INTEGRATION_HUB = "integration-hub"

# Labels used to identify all authorization policies owned by a single workload
# service account, so they can be deleted by selector regardless of the current
# client application list or whether the service mesh is enabled.
WORKLOAD_NAMESPACE_LABEL = "integration-hub/workload-namespace"
WORKLOAD_SERVICE_ACCOUNT_LABEL = "integration-hub/workload-service-account"
