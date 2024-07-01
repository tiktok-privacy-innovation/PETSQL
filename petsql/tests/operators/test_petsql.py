# Copyright 2024 TikTok Pte. Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from petsql.operators import PETSQL

from petsql.tests.utils import PETSQLTestBase


class TestPETSQLOperator(PETSQLTestBase):

    def test_petsql_operator(self, party):
        config = {
            "common": {
                "network_mode":
                    "petnet",
                "network_scheme":
                    "socket",
                "parties": {
                    "party_a": {
                        "address": ["127.0.0.1:8090"]
                    },
                    "party_b": {
                        "address": ["127.0.0.1:8091"]
                    }
                },
                # "network_mode": "petnet",
                # "network_scheme": "agent",
                # "shared_topic": "test_petsql.0620",
                # "parties": {
                #     "party_a": {
                #         "address": ["127.0.0.1:1235"]
                #     },
                #     "party_b": {
                #         "address": ["127.0.0.1:1235"]
                #     }
                # },
                "sql":
                    """
                    SELECT
                        b.f3 as f3,
                        sum(b.f1) as count_f,
                        sum(b.f1 * b.f1 + a.f1 - a.f1 / b.f1) AS sum_f,
                        sum(b.f1 * a.f1 + 1) as sum_f2
                    FROM (select id1, id2, f1 from table_from_a where f1 < 90) AS a
                    JOIN (select id1, id2, f1 + f2 + 2.01 as f1, f1 * f2 + 1 as f2, f3 from table_from_b) AS b ON a.id1 = b.id1
                    GROUP BY b.f3
                    """,
                "config": {
                    "mode": "memory",
                    "schemas": [{
                        "name": "table_from_a",
                        "columns": [{
                            "name": "id1",
                            "type": 2,
                            "party": 0
                        }, {
                            "name": "id2",
                            "type": 2,
                            "party": 0
                        }, {
                            "name": "f1",
                            "type": 3,
                            "party": 0
                        }],
                        "party": 0
                    }, {
                        "name": "table_from_b",
                        "columns": [{
                            "name": "id1",
                            "type": 2,
                            "party": 1
                        }, {
                            "name": "id2",
                            "type": 2,
                            "party": 1
                        }, {
                            "name": "f1",
                            "type": 3,
                            "party": 1
                        }, {
                            "name": "f2",
                            "type": 3,
                            "party": 1
                        }, {
                            "name": "f3",
                            "type": 2,
                            "party": 1
                        }],
                        "party": 1
                    }],
                    "table_url": {
                        "table_from_a": "./tests/test_data/csv/table_from_a.csv",
                        "table_from_b": "./tests/test_data/csv/table_from_b.csv"
                    },
                    "engine_url": "memory:///",
                    "reveal_to": 0,
                    "task_id": "test_task_id"
                }
            },
            "party_a": {
                "inputs": {
                    "data": ["./tests/test_data/csv/table_from_a.csv"]
                },
                "outputs": {
                    "data": ["./tests/test_data/csv/test_ret.csv"]
                }
            },
            "party_b": {
                "inputs": {
                    "data": ["./tests/test_data/csv/table_from_b.csv"]
                },
                "outputs": {
                    "data": []
                }
            }
        }
        if party == 0:
            petsql = PETSQL("party_a")
        else:
            petsql = PETSQL("party_b")
        petsql.run(config, None)
