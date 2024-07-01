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
import numpy as np
import pandas as pd
import petace.securenumpy as snp

from petsql.engine.plain_engine import PlainEngine
from petsql.engine.cipher_engine import CipherEngine
from petsql.data import Data, DataType, PlainMemoryData, PlainBigData, MPCDuetData
from petsql.data.schema import Schema, ColumnType
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
            if src_data.data is None:
                plain_data = None
            else:
                plain_data = self.plain_impl.load_data_from_plain_data(src_data, src_data.schema.name,
                                                                       dst_columns_names)
            return self._df_to_duet(plain_data, dst_table_schema)
        raise DataConverterException(f"not support convert dst_data_type: {dst_data_type}")

    def _mpc_duet_to(self,
                     src_data: "MPCDuetData",
                     dst_data_type: "DataType",
                     dst_table_schema: "Schema",
                     params: dict = None) -> Data:
        dst_columns_names = [column.name for column in dst_table_schema.columns]
        plain_data = pd.DataFrame()

        for column in dst_table_schema.columns:
            tmp_data = src_data.data.get(column.name).reveal_to(column.party.value)
            if column.party.value == self.cipher_impl.duet.party_id():
                tmp_data_df = pd.DataFrame(tmp_data, columns=[column.name])
                plain_data[column.name] = tmp_data_df.astype(ColumnType.to_python_type(column.type))
        if dst_data_type == DataType.PLAIN_MEMORY:
            return PlainMemoryData(plain_data, dst_table_schema)
        if dst_data_type == DataType.PLAIN_BIG_DATA:
            dst_table_name = dst_table_schema.name
            if params is None:
                raise DataConverterException("dst url is not in params.")
            url = params["url"]
            dst_table_write_url = url
            self.plain_impl.save_data(dst_table_write_url, plain_data, dst_table_name, dst_columns_names)
            return PlainBigData(url, dst_table_schema)
        raise DataConverterException(f"not support convert dst_data_type: {dst_data_type}")

    def _same_type_transport(self, src_data: "Data", dst_table_schema: "Schema", params: dict = None) -> Data:
        dst_columns_names = [column.name for column in dst_table_schema.columns]
        if src_data.data_type == DataType.PLAIN_MEMORY:
            if self.plain_impl.mode == Mode.BIGDATA:
                raise DataConverterException("not support convert plain memory data with bigdata plain engine.")
            output_data = src_data.data[dst_columns_names]
            return PlainMemoryData(output_data, dst_table_schema)
        if src_data.data_type == DataType.MPC_DUET:
            output_data = {key: src_data.data.get(key) for key in dst_columns_names}
            return MPCDuetData(output_data, dst_table_schema)
        if src_data.data_type == DataType.PLAIN_BIG_DATA:
            if self.plain_impl.mode == Mode.MEMORY:
                raise DataConverterException("not support convert big data with memory plain engine.")
            if params is None:
                dst_url = src_data.data
            else:
                dst_url = params["url"]
            dst_table_name = dst_table_schema.name
            column_clause = []
            for column in dst_table_schema.columns:
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
            return PlainBigData(dst_url, dst_table_schema)
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
            data_to_concat_tmp = []
            for item in data_to_concat:
                if item:
                    data_to_concat_tmp.append(item.data)
            return PlainMemoryData(pd.concat(data_to_concat_tmp, axis=1)[output_column], output_schema)
        return None
