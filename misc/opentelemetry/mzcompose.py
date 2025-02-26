# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.mzcompose.services import Service

SERVICES = [
    Service(
        "jaeger",
        {
            "image": "jaegertracing/all-in-one:1.36",
            "ports": ["16686:16686", 14268, 14250],
            "allow_host_ports": True,
        },
    ),
    Service(
        "otel-collector",
        {
            "image": "otel/opentelemetry-collector:0.56.0",
            "command": "--config=/etc/otel-collector-config.yaml",
            "ports": [
                1888,  # pprof
                13133,  # health_check
                "4317:4317",  # otlp grpc
                "4318:4318",  # otlp http
                55670,  # zpages
            ],
            "allow_host_ports": True,
            "volumes": ["./otel-collector-config.yaml:/etc/otel-collector-config.yaml"],
            "depends_on": ["jaeger"],
        },
    ),
]
