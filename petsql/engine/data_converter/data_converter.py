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

from typing import List, Tuple
import copy
import json
import uuid
import os
import subprocess

import numpy as np
import pandas as pd
import petace.securenumpy as snp

from petsql.engine.plain_engine import PlainEngine
from petsql.engine.cipher_engine import CipherEngine
from petsql.data import Data, DataType, PlainMemoryData, PlainBigData, MPCDuetData, PlainSparkData, MPCBigdataData
from petsql.data.schema import Schema, ColumnType, Column
from petsql.common import Mode

from .exception import DataConverterException


class DataConverter:
    """
    The class is used to convert data between different data type.

    Parameters
    ----------
    cipher_impl: CipherEngine
        The cipher engine.
    plain_impl: PlainEngine
        The plain engine.

    Attributes
    ----------
    cipher_impl: CipherEngine
        The cipher engine.
    plain_impl: PlainEngine
        The plain engine.
    """

    def __init__(self, cipher_impl: "CipherEngine", plain_impl: "PlainEngine") -> None:
        self.cipher_impl = cipher_impl
        self.plain_impl = plain_impl

    def transport(self,
                  src_data: "Data",
                  dst_data_type: "DataType",
                  dst_table_schema: "Schema",
                  params: dict = None) -> Data:
        """
        Transport data from src_data to dst_data_type.

        Parameters
        ----------
        src_data: Data
            The source data.
        dst_data_type: DataType
            The destination data type.
        dst_table_schema: Schema
            The destination table schema.
        params: dict
            The params.

        Returns
        -------
        Data
            The destination data.
        """
        dst_columns_names = [column.name for column in dst_table_schema.columns]
        if src_data.schema:
            src_columns_names = [column.name for column in src_data.schema.columns]
            for column_name in dst_columns_names:
                if column_name not in src_columns_names:
                    raise DataConverterException(f"column {column_name} is not in schema.")
        if src_data.data_type == dst_data_type:
            return self._same_type_transport(src_data, dst_table_schema, params)
        if src_data.data_type == DataType.PLAIN_MEMORY:
            return self._plain_memory_to(src_data, dst_data_type, dst_table_schema, params)
        if src_data.data_type == DataType.PLAIN_BIG_DATA:
            return self._plain_bigdata_to(src_data, dst_data_type, dst_table_schema, params)
        if src_data.data_type == DataType.MPC_DUET:
            return self._mpc_duet_to(src_data, dst_data_type, dst_table_schema, params)
        if src_data.data_type == DataType.PLAIN_SPARK:
            return self._plain_spark_to(src_data, dst_data_type, dst_table_schema, params)
        if src_data.data_type == DataType.MPC_BIGDATA:
            return self._mpc_bigdata_to(src_data, dst_data_type, dst_table_schema, params)
        raise DataConverterException(f"not support convert type: {src_data.data_type}")

    def _plain_memory_to(self,
                         src_data: "PlainMemoryData",
                         dst_data_type: "DataType",
                         dst_table_schema: "Schema",
                         _: dict = None) -> Data:
        if self.plain_impl.mode == Mode.BIGDATA:
            raise DataConverterException("not support convert plain memory data with bigdata plain engine.")
        if dst_data_type == DataType.PLAIN_BIG_DATA:
            raise DataConverterException("not support convert plain memory data to plain big data.")
        if dst_data_type == DataType.MPC_DUET:
            df = src_data.data
            if src_data.index:
                df = df.sort_values(by=src_data.index, ascending=True)
            return self._df_to_duet(src_data.data, dst_table_schema)
        raise DataConverterException(f"not support convert dst_data_type: {dst_data_type}")

    def _plain_bigdata_to(self,
                          src_data: "PlainBigData",
                          dst_data_type: "DataType",
                          dst_table_schema: "Schema",
                          _: dict = None) -> Data:
        if self.plain_impl.mode == Mode.MEMORY:
            raise DataConverterException("not support convert big data with memory plain engine.")
        if dst_data_type == DataType.PLAIN_MEMORY:
            raise DataConverterException("not support convert plain big data to plain memory data.")
        if dst_data_type == DataType.MPC_DUET:
            dst_columns_names = [column.name for column in dst_table_schema.columns]
            dst_columns = copy.deepcopy(dst_table_schema.columns)
            if src_data.index and src_data.index not in dst_columns_names:
                dst_columns.append(Column(src_data.index, ColumnType.INT, dst_table_schema.party))

            if src_data.data is None:
                plain_data = None
            else:
                plain_data = self.plain_impl.load_data_from_plain_data(src_data, src_data.schema.name, dst_columns)
                if src_data.index:
                    plain_data = plain_data.sort_values(by=src_data.index, ascending=True)

            return self._df_to_duet(plain_data, dst_table_schema)
        raise DataConverterException(f"not support convert dst_data_type: {dst_data_type}")

    def _mpc_duet_to(self,
                     src_data: "MPCDuetData",
                     dst_data_type: "DataType",
                     dst_table_schema: "Schema",
                     params: dict = None) -> Data:
        dst_columns_names = [column.name for column in dst_table_schema.columns]
        dst_columns = copy.deepcopy(dst_table_schema.columns)
        plain_data = pd.DataFrame()

        for column in dst_table_schema.columns:
            tmp_data = src_data.data.get(column.name).reveal_to(column.party.value)
            if column.party.value == self.cipher_impl.duet.party_id():
                tmp_data_df = pd.DataFrame(tmp_data, columns=[column.name])
                plain_data[column.name] = tmp_data_df.astype(ColumnType.to_python_type(column.type))
        if src_data.index and src_data.index in dst_columns_names:
            index = src_data.index
        else:
            index = self._get_index_name(dst_columns_names, dst_table_schema.name)
            plain_data[index] = plain_data.index
            dst_columns.append(Column(src_data.index, ColumnType.INT, dst_table_schema.party))

        if dst_data_type == DataType.PLAIN_MEMORY:
            return PlainMemoryData(plain_data, dst_table_schema, index)
        if dst_data_type == DataType.PLAIN_BIG_DATA:
            dst_table_name = dst_table_schema.name
            if params is None:
                raise DataConverterException("dst url is not in params.")
            url = params["url"]
            dst_table_write_url = url
            self.plain_impl.save_data(dst_table_write_url, plain_data, dst_table_name, dst_columns)
            return PlainBigData(url, dst_table_schema, index)
        raise DataConverterException(f"not support convert dst_data_type: {dst_data_type}")

    # pylint: disable=too-many-statements
    def _same_type_transport(self, src_data: "Data", dst_table_schema: "Schema", params: dict = None) -> Data:
        dst_columns = copy.deepcopy(dst_table_schema.columns)
        columns_name = [item.name for item in dst_table_schema.columns]
        if src_data.index and src_data.index not in columns_name:
            dst_columns.append(Column(src_data.index, ColumnType.INT, dst_table_schema.party))
        dst_columns_names = [column.name for column in dst_columns]

        if src_data.data_type == DataType.PLAIN_MEMORY:
            if self.plain_impl.mode == Mode.BIGDATA:
                raise DataConverterException("not support convert plain memory data with bigdata plain engine.")
            output_data = src_data.data[dst_columns_names]
            return PlainMemoryData(output_data, dst_table_schema, src_data.index)
        if src_data.data_type == DataType.MPC_DUET:
            output_data = {key: src_data.data.get(key) for key in dst_columns_names}
            return MPCDuetData(output_data, dst_table_schema, src_data.index)
        if src_data.data_type == DataType.PLAIN_BIG_DATA:
            if self.plain_impl.mode == Mode.MEMORY:
                raise DataConverterException("not support convert big data with memory plain engine.")
            if params is None:
                dst_url = src_data.data
            else:
                dst_url = params["url"]
            dst_table_name = dst_table_schema.name
            column_clause = []
            for column in dst_columns:
                column_clause.append(f"{column.name} {ColumnType.to_sql_type(column.type)}")
            column_clause = ", ".join(column_clause)
            create_table_sql = f"CREATE TABLE IF NOT EXISTS {dst_table_name}({column_clause});"
            drop_table_sql = f"DROP TABLE IF EXISTS {dst_table_name};"
            insert_clause = ", ".join(dst_columns_names)
            insert_table_sql = f"INSERT INTO {dst_table_name} SELECT {insert_clause} FROM {src_data.schema.name};"
            self.plain_impl.sql_engine.connect(dst_url["engine_url"])
            self.plain_impl.sql_engine.execute(drop_table_sql)
            self.plain_impl.sql_engine.execute(create_table_sql)
            self.plain_impl.sql_engine.execute(insert_table_sql)
            return PlainBigData(dst_url, dst_table_schema, src_data.index)
        if src_data.data_type == DataType.PLAIN_SPARK:
            if self.plain_impl.mode != Mode.SPARK:
                raise DataConverterException("only support convert spark data with spark plain engine.")
            if params is None:
                dst_url = src_data.data
            else:
                dst_url = params["url"]
            dst_table_name = dst_table_schema.name
            column_clause = []
            for column in dst_columns:
                column_clause.append(f"{column.name} {ColumnType.to_sql_type(column.type)}")
            column_clause = ", ".join(column_clause)
            create_table_sql = f"CREATE TABLE IF NOT EXISTS {dst_table_name}({column_clause});"
            drop_table_sql = f"DROP TABLE IF EXISTS {dst_table_name};"
            insert_clause = ", ".join(dst_columns_names)
            insert_table_sql = f"INSERT OVERWRITE TABLE {dst_table_name} SELECT {insert_clause} FROM {src_data.schema.name};"
            self.plain_impl.sql_driver.connect(dst_url)
            self.plain_impl.sql_driver.execute_batch([drop_table_sql, create_table_sql, insert_table_sql])
            return PlainSparkData(dst_url, dst_table_schema, src_data.index)
        if src_data.data_type == DataType.MPC_BIGDATA:
            output_data = {key: src_data.data.get(key) for key in dst_columns_names}
            return MPCBigdataData(output_data, dst_table_schema, src_data.index)

        raise DataConverterException(f"not support convert type: {src_data.data_type}")

    def _df_to_duet(self, src_df, dst_table_schema):
        output_data = {}
        for column in dst_table_schema.columns:
            if column.party.value == self.cipher_impl.duet.party_id():
                tmp_data = src_df[column.name].to_numpy().astype(np.float64).reshape(-1, 1)
            else:
                tmp_data = None
            output_data[column.name] = snp.array(tmp_data, column.party.value)
        return MPCDuetData(output_data, dst_table_schema)

    def _plain_spark_to(self,
                        src_data: "PlainSparkData",
                        dst_data_type: "DataType",
                        dst_table_schema: "Schema",
                        params: dict = None):
        if dst_data_type != DataType.MPC_BIGDATA:
            raise DataConverterException("now only support plain spark data to mpc bigdata.")
        output_data = {}
        if params is None:
            run_mpc = False
        else:
            run_mpc = params["run_mpc"]
        for item in dst_table_schema.columns:
            if item.party.value == self.cipher_impl.duet.vm.party:
                if not src_data.index:
                    raise RuntimeError("plain spark to mpc big data must have index")
                csv_dataset = self.cipher_impl.duet.vm.new_csv_dataset()
                self.plain_impl.data_handler.read(self.plain_impl.sql_driver.url,
                                                  csv_dataset.file_path,
                                                  src_data.schema.name, [item],
                                                  src_data.index,
                                                  header=False)
            else:
                csv_dataset = None
            output_data[item.name] = snp.array(csv_dataset, item.party.value)
        if run_mpc:
            self.cipher_impl.duet.vm.run()
            self.cipher_impl.duet.vm.refine_engine_ctx()
        dst_columns_name = [item.name for item in dst_table_schema.columns]
        if src_data.index in dst_columns_name:
            return MPCBigdataData(output_data, dst_table_schema, src_data.index)
        return MPCBigdataData(output_data, dst_table_schema)

    def _get_index_name(self, columns_name, column_name):
        index_name = column_name + "_index"
        name_change_count = 0
        while index_name in columns_name:
            index_name = f"{index_name}{str(name_change_count)}"
            name_change_count += 1
            if name_change_count > 10000:
                raise RuntimeError("name change count too much.")
        return index_name

    def _mpc_bigdata_to(self,
                        src_data: "MPCBigdataData",
                        dst_data_type: "DataType",
                        dst_table_schema: "Schema",
                        params: dict = None) -> Data:
        if dst_data_type != DataType.PLAIN_SPARK:
            raise DataConverterException("now only support mpc bigdata to plain spark data.")
        # 1.trans every data in mpc data to plain spark and add index
        url = params["url"]
        dst_cloumns_name = [item.name for item in dst_table_schema.columns]
        data_to_conbine = []
        tmp_data = []
        for item in dst_table_schema.columns:
            tmp_data.append(src_data.data[item.name].reveal_to(item.party.value, False))
        for idx, item in enumerate(dst_table_schema.columns):
            if self.cipher_impl.duet.vm.party == item.party.value:
                try:
                    tmp_schema = Schema(name=dst_table_schema.name + item.name, party=item.party, columns=[item])
                    tmp_index_name = self._get_index_name(dst_cloumns_name, item.name)
                    self.plain_impl.data_handler.write(self.plain_impl.sql_driver.url,
                                                       tmp_data[idx].data.file_path,
                                                       tmp_schema.name,
                                                       tmp_schema.columns,
                                                       tmp_index_name,
                                                       header=False)
                    data_to_conbine.append(PlainSparkData(url, tmp_schema, tmp_index_name))

                except Exception as e:
                    print(e)
                    raise e
        ret = self.combine_spark_data(data_to_conbine, dst_table_schema)
        dst_columns_name = [item.name for item in dst_table_schema.columns]
        if src_data.index in dst_columns_name and ret:
            ret.index = src_data.index
        return ret

    def combine_data(self, data_to_concat: List["PlainMemoryData"],
                     output_schema: "Schema") -> Tuple["PlainMemoryData", None]:
        """
        Combine data.

        Parameters
        ----------
        data_to_concat: List[PlainMemoryData]
            The data to concat.
        output_schema: Schema
            The output schema.

        Returns
        -------
        Tuple[PlainMemoryData, None]
            The output data.
        """
        if output_schema.party.value == self.cipher_impl.duet.party_id():
            output_column = [column.name for column in output_schema.columns]
            first_index_type = None
            data_to_concat_tmp = []
            for item in data_to_concat:
                if item:
                    if first_index_type is None:
                        first_index_type = item.data[item.index].dtype
                    tmp_data = item.data
                    tmp_data[item.index] = tmp_data[item.index].astype(first_index_type)
                    tmp_data.set_index(item.index, inplace=True)
                    data_to_concat_tmp.append(item.data)
            return PlainMemoryData(pd.concat(data_to_concat_tmp, axis=1)[output_column], output_schema)
        return None

    def get_schema(self, url: str, table_name: str, columns_name: str) -> str:
        json_data = json.loads(url.split("///")[1])
        tmp_path = json_data["warehouse_dir"]
        parmas = {
            "url": url,
            "table_name": table_name,
            "columns_name": columns_name,
            "output_path": tmp_path + f"/get_schema_output_{uuid.uuid4()}.json"
        }
        try:
            script_path = os.path.dirname(os.path.abspath(__file__)) + "/scripts/run_get_type.py"
            subprocess.run(["python3", script_path, json.dumps(parmas)])
            if os.path.exists(parmas["output_path"]):
                with open(parmas["output_path"], "r") as f:
                    ret = json.loads(f.read())
                if ret["status"] == "success":
                    return ret["index_type_str"]
                raise RuntimeError(ret["err_msg"])
            raise RuntimeError("subprocess error")
        finally:
            if os.path.exists(parmas["output_path"]):
                os.remove(parmas["output_path"])

    def _run_combine_spark_sql(self, url, sqls):
        json_data = json.loads(url.split("///")[1])
        tmp_path = json_data["warehouse_dir"]
        parmas = {"sqls": sqls, "url": url, "output_path": tmp_path + f"/spark_output_{uuid.uuid4()}.json"}
        # pylint: disable=too-many-try-statements
        try:
            script_path = os.path.dirname(os.path.abspath(__file__)) + "/scripts/run_combine_data.py"
            subprocess.run(["python3", script_path, json.dumps(parmas)])
            if os.path.exists(parmas["output_path"]):
                with open(parmas["output_path"], "r") as f:
                    ret = json.loads(f.read())
                if ret["status"] == "success":
                    return None
                raise RuntimeError(ret["err_msg"])
            raise RuntimeError("sql driver subprocess error")
        finally:
            if os.path.exists(parmas["output_path"]):
                os.remove(parmas["output_path"])

    def combine_spark_data(self, data_to_concat: List["PlainSparkData"],
                           output_schema: "Schema") -> Tuple["PlainSparkData", None]:
        if output_schema.party.value == self.cipher_impl.duet.party_id():
            data_to_concat_tmp = []
            for item in data_to_concat:
                if item:
                    data_to_concat_tmp.append(item)
            url = data_to_concat_tmp[0].data
            index = data_to_concat_tmp[0].index
            index_type_str = self.get_schema(url, data_to_concat_tmp[0].schema.name, index)
            columns_to_select = []
            columns_to_creat = []
            columns_to_insert = []
            for idx, data in enumerate(data_to_concat_tmp):
                for column in data.schema.columns:
                    columns_to_select.append(f"t{idx + 1}.{column.name} AS {column.name}")
                    columns_to_creat.append(f"{column.name} {ColumnType.to_spark_sql_type(column.type)}")
                    columns_to_insert.append(
                        f"CAST({column.name} AS {ColumnType.to_spark_sql_type(column.type)}) AS {column.name}")
            columns_to_select.append(f"t1.{index} AS {index}")
            columns_to_creat.append(f"{index} {index_type_str}")
            columns_to_insert.append(index)
            columns_to_select = ", ".join(columns_to_select)
            columns_to_creat = ", ".join(columns_to_creat)
            columns_to_insert = ", ".join(columns_to_insert)
            join_sub_sql = []
            for idx, data in enumerate(data_to_concat_tmp[1:]):
                join_sub_sql.append(
                    f"JOIN {data.schema.name} t{idx + 2} ON CAST(t1.{index} AS {index_type_str}) = CAST(t{idx + 2}.{data.index} AS {index_type_str})"
                )
            join_sub_sql = "\n".join(join_sub_sql)
            sql = f"""
            SELECT {columns_to_select}
            FROM {data_to_concat_tmp[0].schema.name} t1
            {join_sub_sql}
            """

            drop_sql = f"DROP TABLE IF EXISTS {output_schema.name}"

            create_sql = f"CREATE TABLE IF NOT EXISTS {output_schema.name} ({columns_to_creat}) USING parquet"
            insert_sql = f"INSERT OVERWRITE TABLE {output_schema.name} SELECT {columns_to_insert} FROM tmp_insert_table"
            self._run_combine_spark_sql(url, [sql, drop_sql, create_sql, insert_sql])
            return PlainSparkData(url, output_schema, index)
        return None
