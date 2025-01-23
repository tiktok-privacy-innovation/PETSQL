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

from typing import List

import sqlite3
import pandas as pd

from petsql.data import Column, ColumnType
from .abc import AbstractDataHandler


class DBDataHandler(AbstractDataHandler):

    def __parse_path(self, path):
        if "#" not in path:
            raise ValueError(f"path must like <db_name>#<table_name>, not {path}")
        db_name, table_name = path.split("#")
        return db_name, table_name

    # pylint: disable=keyword-arg-before-vararg
    def read(self, path: str, columns: List["Column"] = None, index_column_name=None, *args, **kwargs) -> pd.DataFrame:
        """
        Read db and return a pandas dataframe.

        Parameters
        ----------
        path : str
            dbname#tablename
        columns : List[str]
            _description_
        index_column_name : str
            Index column name of the data. If this name not in the columns name, it will add a index columns with index_column_name.
        *args :
            *args for sqllite
        **kwargs :
            **kwargs for sqllite

        Returns
        -------
        pd.DataFrame
            _description_
        """
        db_name, table_name = self.__parse_path(path)
        con = sqlite3.connect(db_name)
        if columns is None:
            usecols = "*"
        else:
            usecols = [item.name for item in columns]
        sql = f"SELECT {','.join(usecols)} FROM {table_name}"
        data = pd.read_sql(sql, con)
        if columns:
            dtype = {item.name: ColumnType.to_pandas_type(item.type) for item in columns}
            data = data.astype(dtype)
            if index_column_name is not None and index_column_name not in usecols:
                data[index_column_name] = data.index
        con.close()
        return data

    # pylint: disable=keyword-arg-before-vararg
    def write(self,
              path: str,
              data: pd.DataFrame,
              columns: List["Column"] = None,
              index_column_name=None,
              *args,
              **kwargs) -> None:
        """
        Write data to a db.

        Parameters
        ----------
        path : str
            dbname#tablename
        data : pd.DataFrame
            data to write.
        columns : List[str]
            columns to write.
        index_column_name : str
            Index column name of the data. If this name not in the columns name, it will add a index columns with index_column_name.
        *args :
            *args for pandas
        **kwargs :
            **kwargs for pandas

        """
        db_name, table_name = self.__parse_path(path)

        if index_column_name:
            data = data.sort_values(by=index_column_name, ascending=True)
        if columns is not None:
            usecols = [item.name for item in columns]
            data = data[usecols]
        con = sqlite3.connect(db_name)
        data.to_sql(table_name, con, if_exists="replace", index=False)
        con.close()
