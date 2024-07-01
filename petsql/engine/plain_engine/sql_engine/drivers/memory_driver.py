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
from pandasql import sqldf
from .abc import AbstractSqlDriver


class MemorySQLDriver(AbstractSqlDriver):
    kwargs = {}

    def connect(self, url):
        """
        Register a table to the engine.
        Must use keyword Variable Arguments.
        """

    def register(self, **kwargs):
        self.kwargs.update(kwargs)

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
        return sqldf(sql, self.kwargs)
