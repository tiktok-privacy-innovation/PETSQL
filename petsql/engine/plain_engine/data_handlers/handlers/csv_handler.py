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
import pandas as pd
from .abc import AbstractDataHandler


class CsvDataHandler(AbstractDataHandler):

    def read(self, path: str, columns: List[str] = None) -> pd.DataFrame:
        """
        Read csv file and return a pandas dataframe.

        Parameters
        ----------
        path : str
            path of the csv file.
        columns : List[str]
            _description_

        Returns
        -------
        pd.DataFrame
            _description_
        """
        return pd.read_csv(path, usecols=columns)

    def write(self, path: str, data: pd.DataFrame, columns: List[str] = None) -> None:
        """
        Write data to a CSV file.

        Parameters
        ----------
        path : str
            path of the csv file.
        data : pd.DataFrame
            data to write.
        columns : List[str]
            columns to write.
        """
        data.to_csv(path, columns=columns, index=False)
