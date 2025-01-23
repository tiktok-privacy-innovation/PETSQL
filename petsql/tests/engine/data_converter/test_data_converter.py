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

from petsql.tests.utils import PETSQLTestBase, TestConfig
from petsql.data import Schema, DataType, PlainMemoryData, PlainSparkData
from petsql.common import Mode
from petsql.engine.data_converter import DataConverter


class TestDataConvertor(PETSQLTestBase):

    def run_process(self, party):
        self.src_table_schema_str = {
            'name': 'table_from_a',
            'columns': [{
                'name': 'id1',
                'type': 2,
                'party': 0
            }, {
                'name': 'id2',
                'type': 2,
                'party': 0
            }, {
                'name': 'f1',
                'type': 3,
                'party': 0
            }],
            'party': 0
        }
        self.dst_table_schema_str = {
            'name': 'table_from_a_1',
            'columns': [{
                'name': 'id1',
                'type': 2,
                'party': 0
            }, {
                'name': 'f1',
                'type': 3,
                'party': 0
            }],
            'party': 0
        }
        self._init_executor(party, Mode.MEMORY)
        return super().run_process(party)

    def test_data_convertor(self, party):
        data_converter = DataConverter(self.cipher_engine, self.plain_engine)
        path = TestConfig.get_test_data_path(0) + "/csv/table_from_a.csv"
        table_name = "table_from_a"

        src_data_schema = Schema().from_dict(self.src_table_schema_str)
        dst_data_schema = Schema().from_dict(self.dst_table_schema_str)
        data = self.data_handler.read(path, table_name, src_data_schema.columns, header=0)
        src_data = PlainMemoryData(data, src_data_schema)
        output_data = data_converter.transport(src_data, DataType.PLAIN_MEMORY, dst_data_schema)
        assert output_data.data.shape[0] == data.shape[0]
        assert output_data.data.columns.to_list() == ['id1', 'f1']

        if party == 0:
            src_data_schema = Schema().from_dict(self.src_table_schema_str)
            data = data_converter.plain_impl.data_handler.read(path, table_name, src_data_schema.columns, header=0)
        else:
            data = None
            src_data_schema = None
        src_data = PlainMemoryData(data, src_data_schema)
        dst_data = data_converter.transport(src_data, DataType.MPC_DUET, dst_data_schema)
        if party == 0:
            assert dst_data.data["id1"].shape[0] == src_data.data.shape[0]


class TestBigDataConvertor(PETSQLTestBase):

    def run_process(self, party):
        self.src_table_schema_str = {
            'name': 'table_from_a',
            'columns': [{
                'name': 'id1',
                'type': 2,
                'party': 0
            }, {
                'name': 'id2',
                'type': 2,
                'party': 0
            }, {
                'name': 'f1',
                'type': 3,
                'party': 0
            }],
            'party': 0
        }
        self.dst_table_schema_str = {
            'name': 'table_from_a_1',
            'columns': [{
                'name': 'id1',
                'type': 2,
                'party': 0
            }, {
                'name': 'f1',
                'type': 3,
                'party': 0
            }],
            'party': 0
        }
        self._init_spark_executor(party, Mode.SPARK)
        return super().run_process(party)

    def test_data_converter(self, _party):
        src_data_schema = Schema().from_dict(self.src_table_schema_str)
        dst_data_schema = Schema().from_dict(self.dst_table_schema_str)
        data_converter = DataConverter(self.cipher_engine, self.plain_engine)
        table_path = TestConfig.get_test_data_path(0) + "/csv/table_from_a.csv"
        self.data_handler.write(self.plain_engine.sql_driver.url,
                                table_path,
                                "table_from_a",
                                src_data_schema.columns,
                                "id1",
                                header=True)
        src_data = PlainSparkData(self.plain_engine.sql_driver.url, src_data_schema, "id1")
        output_data0 = data_converter.transport(src_data, DataType.PLAIN_SPARK, dst_data_schema)
        output_data0.index = "id1"
        output_data1 = data_converter.transport(output_data0, DataType.MPC_BIGDATA, dst_data_schema, {"run_mpc": True})
        dst_data_schema.name = "test_table_2xx"
        data_converter.transport(output_data1, DataType.PLAIN_SPARK, dst_data_schema,
                                 {"url": self.plain_engine.sql_driver.url})
