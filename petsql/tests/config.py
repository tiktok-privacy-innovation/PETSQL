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

from petsql.data import Column, ColumnType, Party
from petsql.data import Schema
from petsql.common import Config, Mode


class TestSchema:

    @staticmethod
    def get_schema_from_a():
        col0 = Column("id1", ColumnType.INT)
        col1 = Column("id2", ColumnType.INT)
        col2 = Column("f1", ColumnType.DOUBLE)
        schema = Schema("table_from_a", Party.ZERO)
        schema.append_column(col0)
        schema.append_column(col1)
        schema.append_column(col2)
        return schema

    @staticmethod
    def get_schema_from_b():
        col0 = Column("id1", ColumnType.INT)
        col1 = Column("id2", ColumnType.INT)
        col2 = Column("f1", ColumnType.DOUBLE)
        col3 = Column("f2", ColumnType.DOUBLE)
        col4 = Column("f3", ColumnType.INT)
        schema = Schema("table_from_b", Party.ONE)
        schema.append_column(col0)
        schema.append_column(col1)
        schema.append_column(col2)
        schema.append_column(col3)
        schema.append_column(col4)
        return schema


class TestConfig:

    @staticmethod
    def get_config():
        config = Config()
        config.schemas = [TestSchema.get_schema_from_a(), TestSchema.get_schema_from_b()]
        config.table_url = {
            "table_from_a": "./tests/test_data/csv/table_from_a.csv",
            "table_from_b": "./tests/test_data/csv/table_from_b.csv"
        }
        config.engine_url = "memory:///"
        config.reveal_to = Party.ZERO
        config.mode = Mode.MEMORY
        config.task_id = "test_task_id"
        return config

    @staticmethod
    def get_aimed_sql():
        sql = """
        SELECT
            b.f3 as f3,
            sum(b.f1) as sum_f,
            sum(b.f1 + b.f2) as sum_f2,
            max(b.f1 * b.f1 + a.f1 - a.f1 / b.f1) AS max_f,
            min(b.f1 * a.f1 + 1) as min_f
        FROM (select id1, id2, f1 from table_from_a where f1 < 90) AS a
        JOIN (select id1, id2, f1 + f2 + 2.01 as f1, f1 * f2 + 1 as f2, f3 from table_from_b) AS b ON a.id1 = b.id1
        GROUP BY b.f3
        """
        return sql

    @staticmethod
    def get_bigdata_config(party):
        config = Config()
        config.schemas = [TestSchema.get_schema_from_a(), TestSchema.get_schema_from_b()]
        config.table_url = {
            "table_from_a": "./tests/test_data/csv/table_from_a.csv",
            "table_from_b": "./tests/test_data/csv/table_from_b.csv"
        }
        if party == 0:
            config.engine_url = "sqlite:///tests/test_data/db/table_from_a.db"
        else:
            config.engine_url = "sqlite:///tests/test_data/db/table_from_b.db"
        config.reveal_to = Party.ZERO
        config.mode = Mode.BIGDATA
        config.task_id = "test_task_id"
        return config
