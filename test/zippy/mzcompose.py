# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import random

from materialize.mzcompose import Composition, WorkflowArgumentParser
from materialize.mzcompose.services import (
    Kafka,
    Materialized,
    SchemaRegistry,
    Testdrive,
    Zookeeper,
)
from materialize.zippy.framework import Test
from materialize.zippy.scenarios import *  # noqa: F401 F403

SERVICES = [
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    Materialized(),
    Testdrive(
        validate_data_dir=False,
        no_reset=True,
        seed=1,
        default_timeout="600s",
        materialize_params={"statement_timeout": "'900s'"},
    ),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument(
        "--scenario",
        metavar="SCENARIO",
        type=str,
        help="Scenario to run",
        required=True,
    )

    parser.add_argument("--seed", metavar="N", type=int, help="Random seed", default=1)

    parser.add_argument(
        "--actions",
        metavar="N",
        type=int,
        help="Number of actions to run",
        default=1000,
    )

    args = parser.parse_args()
    scenario_class = globals()[args.scenario]

    c.start_and_wait_for_tcp(services=["zookeeper", "kafka", "schema-registry"])
    c.up("testdrive", persistent=True)

    random.seed(args.seed)

    print("Generating test...")
    test = Test(scenario=scenario_class(), actions=args.actions)
    print("Running test...")
    test.run(c)
