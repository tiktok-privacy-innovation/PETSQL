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

import json
import sys

from pyspark.sql import Row
from petsql.data.schema import Column, ColumnType
from petsql.common.spark_utils import get_saprk_session


# pylint: disable=too-many-statements
def run_spark_write():
    params = json.loads(sys.argv[1])
    url = params["url"]
    data = params["data"]
    table_name = params["table_name"]
    columns = params["columns"]
    columns = [Column().from_dict(item) for item in columns]
    index_column_name = params["index_column_name"]
    args = params["args"]
    kwargs = params["kwargs"]
    output_path = params["output_path"]
    output = ""
    # pylint: disable=too-many-try-statements
    try:
        session = get_saprk_session(url)
        if data.split(".")[-1] in {"csv", "tmp"}:
            df = session.read.csv(data, *args, **kwargs)
            if columns:
                column_names = [column.name for column in columns]
                df = df.toDF(*column_names)
        elif data.split(".")[-1] == "parquet":
            df = session.read.parquet(data)
        else:
            raise RuntimeError(f"not support type {data}, only support csv and parquet")
        columns_name = [item.name for item in columns]
        if index_column_name is not None and index_column_name not in columns_name:
            rdd_with_index = df.rdd.zipWithIndex()

            def add_index_column(row, index, index_name):
                row_dict = row.asDict()
                row_dict[index_name] = index
                return Row(**row_dict)

            expanded_rdd = rdd_with_index.map(lambda row: add_index_column(row[0], row[1], index_column_name))

            df = session.createDataFrame(expanded_rdd)
        df.createOrReplaceTempView("tmp_table")
        drop_sql = f"DROP TABLE IF EXISTS {table_name}"
        session.sql(drop_sql)
        columns_sub_sql_list = [f"{item.name} {ColumnType.to_spark_sql_type(item.type)}" for item in columns]
        columns_to_insert = [
            f"CAST({column.name} AS {ColumnType.to_spark_sql_type(column.type)}) AS {column.name}" for column in columns
        ]
        if index_column_name is not None and index_column_name not in columns_name:
            columns_sub_sql_list = columns_sub_sql_list + [f"{index_column_name} INT"]
            columns_to_insert.append(index_column_name)
        columns_to_insert = ", ".join(columns_to_insert)
        create_sub_sql = ", ".join(columns_sub_sql_list)
        create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({create_sub_sql}) USING parquet"
        session.sql(create_sql)
        insert_sql = f"INSERT OVERWRITE TABLE {table_name} SELECT {columns_to_insert} FROM tmp_table"
        session.sql(insert_sql)
        output = json.dumps({"status": "success"})
        # pylint: disable=broad-exception-caught
    except Exception as e:
        output = json.dumps({"status": "failed", "err_msg": str(e)})
    finally:
        if session:
            session.stop()
        with open(output_path, "w") as f:
            f.write(output)


if __name__ == '__main__':
    run_spark_write()
