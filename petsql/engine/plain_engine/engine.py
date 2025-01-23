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

from typing import Union, List, Dict
import pandas as pd

from petsql.data import PlainMemoryData, PlainBigData, PlainSparkData
from petsql.engine.plain_engine.data_handlers import DataHandler
from petsql.engine.plain_engine.sql_engine import AbstractSqlDriver
from petsql.data import Schema, Column
from petsql.common import Mode
from petsql.engine.plain_engine.parser import Parser

from .exception import PlainEngineOperatorException


class PlainEngine:
    """
    PlainEngine is a unified execution engine for PETSQL.
    It is responsible for executing the plain physical execution plan.

    Parameters
    ----------
    data_handler: DataHandler
        DataHandler is a class that is responsible for handling data.
    sql_driver: AbstractSqlDriver
        AbstractSqlDriver is a class that is responsible for executing SQL.
    mode: Mode
        Mode is a enum class that is responsible for indicating the mode of execution.

    Attributes
    ----------
    sql_driver: AbstractSqlDriver
        AbstractSqlDriver is a class that is responsible for executing SQL.
    data_handler: DataHandler
        DataHandler is a class that is responsible for handling data.
    mode: Mode
        Mode is a enum class that is responsible for indicating the mode of execution.
    parser: Parser
        Parser is a class that is responsible for parsing the physical execution plan.
    vm: VM
        petsql.engine.mpc_sql_vm.vm.VM
        VM is a class that is responsible for executing the physical execution plan.
    """

    def __init__(self, data_handler: DataHandler, sql_driver: AbstractSqlDriver, mode: Mode) -> None:
        self.sql_driver = sql_driver
        self.data_handler = data_handler
        self.mode = mode
        self.parser = Parser()
        self.vm = None

    def set_vm(self, vm: "VM") -> None:
        """
        Set the virtual machine object.

        Parameters
        ----------
        vm: VM
            VM is a class that is responsible for executing the physical execution plan.
        """
        self.vm = vm

    def table_scan(self, physical_plan: Dict) -> Union[PlainMemoryData, PlainBigData]:
        """
        Load table from url in physical_plan.

        Parameters
        ----------
        physical_plan: Dict
            Physical execution plan.

        Returns
        -------
        Union[PlainMemoryData, PlainBigData]
            PlainMemoryData or PlainBigData object.
        """
        output_schema = Schema().from_dict(physical_plan["outputs"][0])
        table_name = physical_plan["inputs"]["table"][0]["name"]
        url = physical_plan["inputs"]["url"][table_name]
        if self.mode == Mode.SPARK:
            self.data_handler.write(self.sql_driver.url,
                                    url,
                                    output_schema.name,
                                    output_schema.columns,
                                    None,
                                    header=True)
            return PlainSparkData(self.sql_driver.url, output_schema)
        file_type = url.split(".")[-1]
        if file_type in {"csv", "parquet"}:
            data = self.data_handler.read(url, table_name, output_schema.columns, header=0)
            if self.mode == Mode.MEMORY:
                return PlainMemoryData(data, output_schema)
            if self.mode == Mode.BIGDATA:
                url = self.sql_driver.url.split("///")[1]
                self.save_data(url, data, table_name, output_schema.columns)
                output_schema.name = table_name
                return PlainBigData(url, output_schema)
            raise PlainEngineOperatorException(f"Unsupported mode: {self.mode}")
        output_schema.name = table_name
        return PlainBigData(url, output_schema)

    def load_data_from_plain_data(self,
                                  data: Union[PlainMemoryData, PlainBigData],
                                  table_name: str,
                                  columns: List["Column"] = None) -> pd.DataFrame:
        """
        Load data from plain data.

        Parameters
        ----------
        data: Union[PlainMemoryData, PlainBigData]
            PlainMemoryData or PlainBigData object.
        table_name: str
            Table name.
        columns: List[str]
            Column names.

        Returns
        -------
        pd.DataFrame
            Data in plain data.
        """
        if isinstance(data, PlainMemoryData):
            if columns is None:
                select_columns = [column.name for column in data.schema.columns]
            else:
                select_columns = [column.name for column in columns]
            return data.data[select_columns]
        if isinstance(data, PlainBigData):
            data = self.data_handler.read(data.data, table_name, columns)
            return data

        return None

    def save_data(self, url: str, data: pd.DataFrame, table_name: str, columns: List[Column] = None) -> None:
        """
        Save data according to the url.

        Parameters
        ----------
        url: str
            The data where to write.
        data: pd.DataFrame
            Data to be saved.
        table_name: str
            Table name.
        columns: List[str]
            Column names.
        """
        self.data_handler.write(url, data, table_name, columns)

    def project(self, data: "RegisterItem", plain_plan: Dict) -> Union[PlainMemoryData, PlainBigData]:
        """
        Project the data according to the physical plan.

        Parameters
        ----------
        plain_plan: dict
            Physical plan.
        data: RegisterItem
            Data to be projected.

        Returns
        -------
        Union[PlainMemoryData, PlainBigData]
            Projected data.
        """
        plain_data = data.plain_data
        last_schema = data.schema
        if plain_plan["expressions"]:
            sql, output_table_schema = self.parser.parse_project_plan_to_sql(plain_plan, last_schema, self.mode,
                                                                             plain_data.index)
            input_table_name = last_schema.name
            if isinstance(plain_data, PlainMemoryData) and self.mode == Mode.MEMORY:
                content = {f'{input_table_name}': plain_data.data}
                self.sql_driver.register(**content)
                res_data = self.sql_driver.execute(sql)
                return PlainMemoryData(res_data, output_table_schema, plain_data.index)
            if isinstance(plain_data, PlainBigData) and self.mode == Mode.BIGDATA:
                url = plain_data.data
                create_table_sql = self.parser.create_output_table_sql_from_plan(plain_plan, self.mode,
                                                                                 plain_data.index)
                drop_table_sql = f'DROP TABLE IF EXISTS {output_table_schema.name};'
                self.sql_driver.execute(drop_table_sql)
                self.sql_driver.execute(create_table_sql)
                self.sql_driver.execute(sql)
                return PlainBigData(url, output_table_schema, plain_data.index)
            if isinstance(plain_data, PlainSparkData) and self.mode == Mode.SPARK:
                url = plain_data.data
                create_table_sql = self.parser.create_output_table_sql_from_plan(plain_plan, self.mode,
                                                                                 plain_data.index)
                drop_table_sql = f'DROP TABLE IF EXISTS {output_table_schema.name};'
                self.sql_driver.execute_batch([drop_table_sql, create_table_sql, sql])
                return PlainSparkData(url, output_table_schema, plain_data.index)
            raise PlainEngineOperatorException("Data and mode is not consistent.")
        return None

    def filter(self, data: "RegisterItem", plain_plan: dict) -> Union[PlainMemoryData, PlainBigData]:
        """
        Filter the data according to the physical plan.

        Parameters
        ----------
        data: RegisterItem
            Data to be filtered.
        plain_plan: dict
            Physical plan.

        Returns
        -------
        Union[PlainMemoryData, PlainBigData]
            Filtered data.
        """
        plain_data = data.plain_data
        last_schema = data.schema
        sql, output_table_schema = self.parser.parse_filter_plan_to_sql(plain_plan, last_schema, self.mode)
        input_table_name = last_schema.name
        if isinstance(plain_data, PlainMemoryData) and self.mode == Mode.MEMORY:
            content = {f'{input_table_name}': plain_data.data}
            self.sql_driver.register(**content)
            res_data = self.sql_driver.execute(sql)
            return PlainMemoryData(res_data, output_table_schema, plain_data.index)
        if isinstance(plain_data, PlainBigData) and self.mode == Mode.BIGDATA:
            url = plain_data.data
            create_table_sql = self.parser.create_output_table_sql_from_plan(plain_plan, self.mode, plain_data.index)
            drop_table_sql = f'DROP TABLE IF EXISTS {output_table_schema.name};'
            self.sql_driver.execute(drop_table_sql)
            self.sql_driver.execute(create_table_sql)
            self.sql_driver.execute(sql)
            return PlainBigData(url, output_table_schema, plain_data.index)
        if isinstance(plain_data, PlainSparkData) and self.mode == Mode.SPARK:
            url = plain_data.data
            create_table_sql = self.parser.create_output_table_sql_from_plan(plain_plan, self.mode, plain_data.index)
            drop_table_sql = f'DROP TABLE IF EXISTS {output_table_schema.name};'
            self.sql_driver.execute_batch([drop_table_sql, create_table_sql, sql])
            return PlainSparkData(url, output_table_schema, plain_data.index)
        raise PlainEngineOperatorException("Data and mode is not consistent.")

    def join(self, data: List["RegisterItem"], plain_plan: Dict) -> Union[PlainMemoryData, PlainBigData]:
        """
        Join the data according to the physical plan.

        Parameters
        ----------
        data: List["RegisterItem"]
            Data to be joined.
        plain_plan: dict
            Physical plan.

        Returns
        -------
        Union[PlainMemoryData, PlainBigData]
            Joined data.
        """
        plain_data = [item.plain_data for item in data]
        last_schemas = [item.schema for item in data]
        sql, output_table_schema = self.parser.parse_join_plan_to_sql(plain_plan, last_schemas, self.mode)
        if isinstance(plain_data[0], PlainMemoryData) and self.mode == Mode.MEMORY:
            content = {f'{last_schemas[0].name}': plain_data[0].data, f'{last_schemas[1].name}': plain_data[1].data}
            self.sql_driver.register(**content)
            res_data = self.sql_driver.execute(sql)
            output_table_columns_name = [column.name for column in output_table_schema.columns]
            res_data.rename(columns=dict(zip(res_data.columns, output_table_columns_name)), inplace=True)
            return PlainMemoryData(res_data, output_table_schema, plain_data[0].index)
        if isinstance(plain_data[0], PlainBigData) and self.mode == Mode.BIGDATA:
            url = plain_data[0].data
            create_table_sql = self.parser.create_output_table_sql_from_plan(plain_plan, self.mode)
            drop_table_sql = f'DROP TABLE IF EXISTS {output_table_schema.name};'
            self.sql_driver.execute(drop_table_sql)
            self.sql_driver.execute(create_table_sql)
            self.sql_driver.execute(sql)
            return PlainBigData(url, output_table_schema, plain_data[0].index)
        if isinstance(plain_data[0], PlainSparkData) and self.mode == Mode.SPARK:
            url = plain_data[0].data
            create_table_sql = self.parser.create_output_table_sql_from_plan(plain_plan, self.mode)
            drop_table_sql = f'DROP TABLE IF EXISTS {output_table_schema.name};'
            self.sql_driver.execute_batch([drop_table_sql, create_table_sql, sql])
            return PlainSparkData(url, output_table_schema, plain_data[0].index)
        raise PlainEngineOperatorException("Data and mode is not consistent.")

    def aggregate(self, data: "RegisterItem", plain_plan: Dict) -> Union[PlainMemoryData, PlainBigData]:
        """
        Aggregate the data according to the physical plan.

        Parameters
        ----------
        data: Union[PlainMemoryData, PlainBigData]
            Data to be aggregated.
        plain_plan: dict
            Physical plan.

        Returns
        -------
        Union[PlainMemoryData, PlainBigData]
            Aggregated data.
        """
        plain_data = data.plain_data
        last_schema = data.schema
        sql, output_table_schema = self.parser.parse_aggregate_plan_to_sql(plain_plan, last_schema, self.mode)
        input_table_name = last_schema.name
        if isinstance(plain_data, PlainMemoryData) and self.mode == Mode.MEMORY:
            content = {f'{input_table_name}': plain_data.data}
            self.sql_driver.register(**content)
            res_data = self.sql_driver.execute(sql)
            return PlainMemoryData(res_data, output_table_schema)
        if isinstance(plain_data, PlainBigData) and self.mode == Mode.BIGDATA:
            url = plain_data.data
            create_table_sql = self.parser.create_output_table_sql_from_plan(plain_plan, self.mode)
            drop_table_sql = f'DROP TABLE IF EXISTS {output_table_schema.name};'
            self.sql_driver.execute(drop_table_sql)
            self.sql_driver.execute(create_table_sql)
            self.sql_driver.execute(sql)
            return PlainBigData(url, output_table_schema)
        if isinstance(plain_data, PlainSparkData) and self.mode == Mode.SPARK:
            url = plain_data.data
            create_table_sql = self.parser.create_output_table_sql_from_plan(plain_plan, self.mode)
            drop_table_sql = f'DROP TABLE IF EXISTS {output_table_schema.name};'
            self.sql_driver.execute_batch([drop_table_sql, create_table_sql, sql])
            return PlainSparkData(url, output_table_schema)
        raise PlainEngineOperatorException("Data and mode is not consistent.")
