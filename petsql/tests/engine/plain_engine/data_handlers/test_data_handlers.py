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

import os
import tempfile
import shutil
import pandas as pd

from petsql.engine.plain_engine.data_handlers import DataHandler, SparkDataHandler

from petsql.tests.utils import CommonTestBase
from petsql.tests.config import TestSchema, TestConfig


class TestDataHandlers(CommonTestBase):

    def test_data_handlers(self):
        test_dir = tempfile.mkdtemp()
        data_handler = DataHandler()
        root_path = os.path.dirname(os.path.abspath(__file__))
        data_path = f"{root_path}/../../../test_data/"
        schema_a = TestSchema.get_schema_from_a()
        name_a = schema_a.name
        data_0 = data_handler.read(f"{data_path}csv/table_from_a.csv", name_a, schema_a.columns, "index", header=0)
        print(data_0)
        data_handler.write(f"{test_dir}/res.csv", data_0, name_a, schema_a.columns, None, header=True)

        data_1 = data_handler.read(f"{data_path}parquet/table_from_a.parquet", name_a, schema_a.columns, "index")
        print(data_1)
        data_handler.write(f"{test_dir}/res.parquet", data_0, name_a, schema_a.columns, None)

        data_2 = data_handler.read(f"{data_path}db/table_from_a.db", name_a, schema_a.columns, "index")
        print(data_2)
        data_handler.write(f"{test_dir}/res.db", data_2, name_a, schema_a.columns, "index")

        assert ((data_0 - data_1).abs() < 0.001).all().all()
        assert ((data_0 - data_2).abs() < 0.001).all().all()

        shutil.rmtree(test_dir)

    def test_spark_handlers_case0(self):
        spark_url = TestConfig.get_test_spark_url(0)
        schema = TestSchema.get_schema_from_a()
        table_path = TestConfig.get_test_data_path(0) + "/csv/table_from_a.csv"
        SparkDataHandler.write(spark_url, table_path, "table_from_a", schema.columns, "id1", header=True)
        tmp_table_path = TestConfig.get_tmp_data_path(0) + "/csv/table_from_a.csv"
        SparkDataHandler.read(spark_url, tmp_table_path, "table_from_a", schema.columns, "id1", header=True)
        aim_df = pd.read_csv(table_path)
        real_df = pd.read_csv(tmp_table_path)
        assert aim_df.equals(real_df)

    def test_spark_handlers_case1(self):
        spark_url = TestConfig.get_test_spark_url(0)
        schema = TestSchema.get_schema_from_a()
        table_path = TestConfig.get_test_data_path(0) + "/csv/table_from_a.csv"
        SparkDataHandler.write(spark_url, table_path, "table_from_a", schema.columns, None, header=True)
        tmp_table_path = TestConfig.get_tmp_data_path(0) + "/csv/table_from_a.csv"
        SparkDataHandler.read(spark_url, tmp_table_path, "table_from_a", schema.columns, None, header=True)
        aim_df = pd.read_csv(table_path)
        real_df = pd.read_csv(tmp_table_path)
        assert aim_df.equals(real_df)

    def test_spark_handlers_case2(self):
        spark_url = TestConfig.get_test_spark_url(0)
        schema = TestSchema.get_schema_from_a()
        table_path = TestConfig.get_test_data_path(0) + "/csv/table_from_a.csv"
        SparkDataHandler.write(spark_url, table_path, "table_from_a", schema.columns, "id", header=True)
        tmp_table_path = TestConfig.get_tmp_data_path(0) + "/csv/table_from_a.csv"
        SparkDataHandler.read(spark_url, tmp_table_path, "table_from_a", schema.columns, "id", header=True)
        aim_df = pd.read_csv(table_path)
        real_df = pd.read_csv(tmp_table_path)
        assert aim_df.equals(real_df)
