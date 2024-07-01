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

from .abc import AbstractDataHandler


class DBDataHandler(AbstractDataHandler):

    def __parse_path(self, path):
        if "#" not in path:
            raise ValueError(f"path must like <db_name>#<table_name>, not {path}")
        db_name, table_name = path.split("#")
        return db_name, table_name

    def read(self, path: str, columns: List[str] = None) -> pd.DataFrame:
        """
        Read db and return a pandas dataframe.

        Parameters
        ----------
        path : str
            dbname#tablename
        columns : List[str]
            _description_

        Returns
        -------
        pd.DataFrame
            _description_
        """
        db_name, table_name = self.__parse_path(path)
        con = sqlite3.connect(db_name)
        if columns is None:
            columns = "*"
        sql = f"SELECT {','.join(columns)} FROM {table_name}"
        data = pd.read_sql(sql, con)
        con.close()
        return data

    def write(self, path: str, data: pd.DataFrame, columns: List[str] = None) -> None:
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
        """
        db_name, table_name = self.__parse_path(path)
        if columns is not None:
            data = data[columns]
        con = sqlite3.connect(db_name)
        data.to_sql(table_name, con, if_exists="replace", index=False)
        con.close()
