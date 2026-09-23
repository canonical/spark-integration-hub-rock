#!/bin/bash
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

python3 -m app \
--app-name=integration-hub \
--config=${SPARK_PROPERTIES_FILE} \
--allowlist=${SA_ALLOWLIST} \
--truststore=${TRUSTSTORE_PATH} \
--truststore-secret-name=${TRUSTSTORE_SECRET_NAME} \
--timeout=30
