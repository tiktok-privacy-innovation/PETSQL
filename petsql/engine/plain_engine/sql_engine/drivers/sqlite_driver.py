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

import pandas as pd
from sqlalchemy import create_engine, text
from .abc import AbstractSqlDriver


class SqliteDriver(AbstractSqlDriver):
    engine = None
    con = None
    url = None

    def connect(self, url):
        self.engine = create_engine(url)
        self.con = self.engine.connect()
        self.url = url

    def __del__(self):
        if self.con:
            self.con.close()

    def execute(self, sql: str) -> pd.DataFrame:
        """
        Execute a sql statement.

        Parameters
        ----------
        sql : str
            sql statement.

        Returns
        -------
        pd.DataFrame
            result of sql statement.
        """
        res = self.con.execute(text(sql))
        if self.con:
            self.con.commit()
        if res.returns_rows == 0:
            return None
        return pd.DataFrame(res.all(), columns=res.keys())
