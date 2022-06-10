# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import os

from materialize import spawn
from materialize.mzcompose import Composition
from materialize.mzcompose.services import Kafka, Postgres, SchemaRegistry, Zookeeper

SERVICES = [
    Zookeeper(),
    Kafka(
        # We need a stable port to advertise, so pick one that is unlikely to
        # conflict with a Kafka cluster running on the local machine.
        port="30123:9092",
        allow_host_ports=True,
        environment_extra=[
            f"KAFKA_ADVERTISED_LISTENERS=HOST://localhost:30123,PLAINTEXT://kafka:9092",
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=HOST:PLAINTEXT,PLAINTEXT:PLAINTEXT",
        ],
    ),
    SchemaRegistry(),
    Postgres(image="postgres:14.2"),
]


def workflow_default(c: Composition) -> None:
    c.start_and_wait_for_tcp(["zookeeper", "kafka", "schema-registry", "postgres"])
    # Heads up: this intentionally runs on the host rather than in a Docker
    # image. See #13010.
    postgres_url = (
        f"postgres://postgres:postgres@localhost:{c.default_port('postgres')}"
    )
    spawn.runv(
        ["cargo", "nextest", "run"],
        env=dict(
            os.environ,
            ZOOKEEPER_ADDR=f"localhost:{c.default_port('zookeeper')}",
            KAFKA_ADDRS=f"localhost:30123",
            SCHEMA_REGISTRY_URL=f"http://localhost:{c.default_port('schema-registry')}",
            POSTGRES_URL=postgres_url,
            MZ_SOFT_ASSERTIONS="1",
            MZ_PERSIST_EXTERNAL_STORAGE_TEST_S3_BUCKET="mtlz-test-persist-1d-lifecycle-delete",
            MZ_PERSIST_EXTERNAL_STORAGE_TEST_POSTGRES_URL=postgres_url,
        ),
    )
