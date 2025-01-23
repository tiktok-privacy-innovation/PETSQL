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

from enum import Enum
from typing import List, Dict
import pandas as pd
from pyspark.sql.types import StringType, BooleanType, IntegerType, DoubleType

from .exception import ColumnTypeException, SchemaException


class ColumnType(Enum):
    CHAR = 0
    BOOLEAN = 1
    INT = 2
    DOUBLE = 3

    @staticmethod
    def to_python_type(input_type) -> type:
        """
        Convert a column type to a Python type.

        Parameters
        ----------
        input_type : ColumnType or int
            The column type to be converted.

        Returns
        -------
        type
            The corresponding Python type.

        Raises
        ------
        ColumnTypeException
            If the input type is not supported.
        """
        if isinstance(input_type, int):
            input_type = ColumnType(input_type)
        if input_type == ColumnType.CHAR:
            return str
        if input_type == ColumnType.INT:
            return int
        if input_type == ColumnType.DOUBLE:
            return float
        if input_type == ColumnType.BOOLEAN:
            return bool
        raise ColumnTypeException("unsupport input type: " + str(input_type))

    @staticmethod
    def to_pandas_type(input_type) -> type:
        """
        Convert a column type to a Pandas type.

        Parameters
        ----------
        input_type : ColumnType or int
            The column type to be converted.

        Returns
        -------
        type
            The corresponding Python type.

        Raises
        ------
        ColumnTypeException
            If the input type is not supported.
        """
        if isinstance(input_type, int):
            input_type = ColumnType(input_type)
        if input_type == ColumnType.CHAR:
            return pd.StringDtype()
        if input_type == ColumnType.INT:
            return pd.Int64Dtype()
        if input_type == ColumnType.DOUBLE:
            return pd.Float64Dtype()
        if input_type == ColumnType.BOOLEAN:
            return pd.BooleanDtype()
        raise ColumnTypeException("unsupport input type: " + str(input_type))

    @staticmethod
    def to_sql_type(input_type) -> str:
        """
        Convert a column type to a SQL type.

        Parameters
        ----------
        input_type : ColumnType or int
            The column type to be converted.

        Returns
        -------
        str
            The corresponding SQL type.

        Raises
        ------
        ColumnTypeException
            If the input type is not supported.
        """
        if isinstance(input_type, int):
            input_type = ColumnType(input_type)
        if input_type == ColumnType.CHAR:
            return "TEXT"
        if input_type == ColumnType.INT:
            return "INTEGER"
        if input_type == ColumnType.DOUBLE:
            return "DOUBLE"
        if input_type == ColumnType.BOOLEAN:
            return "BOOLEAN"
        raise ColumnTypeException("unsupport input type: " + str(input_type))

    @staticmethod
    def to_spark_type(input_type) -> str:
        """
        Convert a column type to a Spark type.

        Parameters
        ----------
        input_type : ColumnType or int
            The column type to be converted.

        Returns
        -------
        str
            The corresponding Spark type.

        Raises
        ------
        ColumnTypeException
            If the input type is not supported.
        """
        if isinstance(input_type, int):
            input_type = ColumnType(input_type)
        if input_type == ColumnType.CHAR:
            return StringType()
        if input_type == ColumnType.INT:
            return IntegerType()
        if input_type == ColumnType.DOUBLE:
            return DoubleType()
        if input_type == ColumnType.BOOLEAN:
            return BooleanType()
        raise ColumnTypeException("unsupport input type: " + str(input_type))

    @staticmethod
    def to_spark_sql_type(input_type) -> str:
        """
        Convert a column type to a Spark SQL type.

        Parameters
        ----------
        input_type : ColumnType or int
            The column type to be converted.

        Returns
        -------
        str
            The corresponding SQL type.

        Raises
        ------
        ColumnTypeException
            If the input type is not supported.
        """
        if isinstance(input_type, int):
            input_type = ColumnType(input_type)
        if input_type == ColumnType.CHAR:
            return "STRING"
        if input_type == ColumnType.INT:
            return "INT"
        if input_type == ColumnType.DOUBLE:
            return "DOUBLE"
        if input_type == ColumnType.BOOLEAN:
            return "BOOLEAN"
        raise ColumnTypeException("unsupport input type: " + str(input_type))


class Party(Enum):
    ZERO = 0
    ONE = 1
    PUBLIC = 2
    SHARE = -1


class Column:
    """
    The meta info of a column of a table.

    Parameters
    ----------
    name : str, default=None
        The name of this column.
    type : ColumnType, default=None
        The data type of this column.
    party : Party, default=None
        The party of this column.

    Attributes
    ----------
    name : str
        The name of this column.
    type : ColumnType
        The data type of this column.
    party : Party
        The party of this column.
    """

    def __init__(self, name: str = None, type: "ColumnType" = None, party: "Party" = None) -> None:
        self.name = name
        self.type = type
        self.party = party

    def to_dict(self) -> Dict:
        """
        Convert the current column object into a dictionary.

        Returns
        -------
        Dict
            A dictionary containing the column object's properties.

        Raises
        ------
        RuntimeError
            If the column object is not properly initialized.
        """
        if self.name is None or self.type is None:
            raise RuntimeError("Column should be inited")
        if self.party is None:
            return {"name": self.name, "type": self.type.value, "party": self.party}
        return {"name": self.name, "type": self.type.value, "party": self.party.value}

    def from_dict(self, input_dict: dict) -> "Column":
        """
        Convert a dictionary into an column object".

        Parameters
        ----------
        input_dict : Dict
            A dictionary containing the column object's properties.

        Returns
        -------
        Column
            The column object with properties set according to the dictionary.

        Raises
        ------
        ColumnTypeException
            If the input type is not supported for the column object.
        """
        self.name = input_dict.get("name")
        if input_dict.get("type") is None:
            self.type = None
        else:
            self.type = ColumnType(input_dict.get("type"))
        if input_dict.get("party") is None:
            self.party = None
        else:
            self.party = Party(input_dict.get("party"))
        return self


class Schema:
    """
    The meta info of a table. The meta info of column is in Column.

    Parameters
    ----------
    name : str, default=None
        The name of this column.
    columns : [Column, ...], default=None
        The columns of this table.
    party : Party, default=None
        The party of this table.

    Attributes
    ----------
    name : str
        The name of this column.
    columns : [Column, ...]
         The columns of this table.
    party : Party
        The party of this table.
    """

    def __init__(self, name: str = None, party: "Party" = None, columns: List["Column"] = None) -> None:
        self.name = name
        self.party = party
        if columns:
            self.columns = columns
        else:
            self.columns = []

    def to_dict(self) -> Dict:
        """
        Convert the current schema object into a dictionary.

        Returns
        -------
        Dict
            A dictionary containing the schema object's properties.

        Raises
        ------
        RuntimeError
            If the schema object is not properly initialized.
        """
        if self.name is None or self.columns is None or self.party is None:
            raise RuntimeError("Schema should be inited")
        columns = [item.to_dict() for item in self.columns]
        if self.party is None:
            return {"name": self.name, "columns": columns, "party": self.party}
        return {"name": self.name, "columns": columns, "party": self.party.value}

    def from_dict(self, input_dict: Dict) -> "Schema":
        """
        Convert a dictionary into an schema object".

        Parameters
        ----------
        input_dict : Dict
            A dictionary containing the schema object's properties.

        Returns
        -------
        Schema
            The schema object with properties set according to the dictionary.
        """
        self.name = input_dict.get("name")

        if input_dict.get("columns") is None:
            self.columns = []
        else:
            columns = []
            for item in input_dict.get("columns"):
                columns.append(Column().from_dict(item))
            self.columns = columns
        if input_dict.get("party") is None:
            self.party = None
        else:
            self.party = Party(input_dict.get("party"))
        return self

    def append_column(self, column: "Column") -> "Schema":
        """
        Append a column to the schema.

        Parameters
        ----------
        column : Column
            The column to append.

        Returns
        -------
        Schema
            The schema after the column is appended.

        Raises
        ------
        SchemaException
            If the column's party is not set and the table is shared.
        """
        # table party = first column's party
        if self.party is None and column.party is not None:
            self.party = column.party
        if column.party is None:
            if self.party != Party.SHARE:
                column.party = self.party
            else:
                raise SchemaException("colunm's party not set, but the table is shared")
        if column.party != self.party:
            self.party = Party.SHARE
        self.columns.append(column)
        return self
