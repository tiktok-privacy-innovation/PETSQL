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
from random import randint, uniform
from typing import List

import pandas as pd

from petsql.data.schema import Schema, ColumnType
from petsql.engine.plain_engine.data_handlers import DataHandler
from petsql.tests.config import TestSchema


class TestDataGenerator:

    @staticmethod
    def gen_test_df(schema: "Schema", number: int, group_by_column: List[str] = None, group_by_range: List[int] = None):
        ret = []
        for i in range(number):
            row = []
            for item in schema.columns:
                if item.type == ColumnType.INT:
                    if "ID" in item.name or "id" in item.name:
                        row.append(i)
                    elif group_by_column is not None and item.name in group_by_column:
                        row.append(randint(0, group_by_range[group_by_column.index(item.name)]))
                    else:
                        row.append(randint(0, pow(2, 32) - 1))
                elif item.type == ColumnType.DOUBLE:
                    row.append(uniform(-100.0, 100.0))
                else:
                    raise RuntimeError(f"unsupport type: {item.types}")
            ret.append(row)
        return pd.DataFrame(ret, columns=[column.name for column in schema.columns])


if __name__ == "__main__":
    data_handler = DataHandler()
    root_path = os.path.dirname(os.path.abspath(__file__))
    schema_a = TestSchema.get_schema_from_a()
    df_a = TestDataGenerator.gen_test_df(schema_a, 10)
    name_a = schema_a.name
    columns_a = [column.name for column in schema_a.columns]
    os.makedirs(f"{root_path}/csv", exist_ok=True)
    os.makedirs(f"{root_path}/parquet", exist_ok=True)
    os.makedirs(f"{root_path}/db", exist_ok=True)
    data_handler.write(f"{root_path}/csv/{name_a}.csv", df_a, name_a, columns=columns_a)
    data_handler.write(f"{root_path}/parquet/{name_a}.parquet", df_a, name_a, columns=columns_a)
    data_handler.write(f"{root_path}/db/{name_a}.db", df_a, name_a, columns=columns_a)

    schema_b = TestSchema.get_schema_from_b()
    df_b = TestDataGenerator.gen_test_df(schema_b, 20, ["f3"], [5])
    name_b = schema_b.name
    columns_b = [column.name for column in schema_b.columns]
    data_handler.write(f"{root_path}/csv/{name_b}.csv", df_b, name_b, columns=columns_b)
    data_handler.write(f"{root_path}/parquet/{name_b}.parquet", df_b, name_b, columns=columns_b)
    data_handler.write(f"{root_path}/db/{name_b}.db", df_b, name_b, columns=columns_b)
