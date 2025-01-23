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

import pandas as pd

from petsql.engine.plain_engine.sql_engine import SqlEngineFactory
from petsql.engine.plain_engine.data_handlers import SparkDataHandler
from petsql.tests.utils import CommonTestBase
from petsql.tests.config import TestSchema, TestConfig


class TestSQLEngine(CommonTestBase):

    def test_memory(self):
        url = "memory:///"
        engine = SqlEngineFactory.create_engine(url)
        df = pd.read_csv("tests/test_data/csv/table_from_a.csv")
        engine.register(table1=df)
        res = engine.execute("select * from table1")
        res = engine.execute("select * from table1 limit 10")
        assert len(res) == 10
        # inner join
        engine.register(table1=df, table2=df)
        res = engine.execute("select * from table1 join table2 on table1.id1=table2.id1")
        assert len(res) == df.shape[0]

    def test_sqllite(self):
        url = "sqlite:///tests/test_data/db/table_from_a.db"
        engine = SqlEngineFactory.create_engine(url)
        res = engine.execute("select * from table_from_a limit 10")
        assert res.shape[0] == 10

    def test_spark(self):
        spark_url = TestConfig.get_test_spark_url(0)
        spark_engine = SqlEngineFactory.create_engine(spark_url)
        schema = TestSchema.get_schema_from_a()
        table_path = TestConfig.get_test_data_path(0) + "/csv/table_from_a.csv"
        SparkDataHandler.write(spark_url, table_path, "table_from_a", schema.columns, "id1", header=True)
        spark_engine.execute("select id1 from table_from_a limit 10", True)
