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
from .handlers import CsvDataHandler, ParquetDataHandler, DBDataHandler

HandlerMap = {
    "csv": CsvDataHandler,
    "parquet": ParquetDataHandler,
    "db": DBDataHandler,
}


def get_handler(path):
    file_type = path.split(".")[-1]
    handler = HandlerMap.get(file_type, None)()
    if not handler:
        raise TypeError(f"Unsupported file type: {file_type}")
    return handler


class DataHandler:

    @staticmethod
    def read(path: str, table_name: str, columns: List = None) -> pd.DataFrame:
        """read data from path, returns PlainData object

        Parameters
        ----------
        path : str
            data path.
        table_name : str
            table name.
        columns : List, optional
            Columns to write.

        Returns
        -------
        pd.DataFrame
        """
        handler = get_handler(path)
        if isinstance(handler, DBDataHandler):
            path = f"{path}#{table_name}"
        df = handler.read(path, columns)
        return df

    @staticmethod
    def write(path: str, obj: pd.DataFrame, table_name: str, columns=None) -> None:
        """write data to path.

        Parameters
        ----------
        path : str
            data path.
        obj : pd.DataFrame
            data to write.
        table_name : str
            table name.
        columns : List, optional
            Columns to write.
        """
        if not isinstance(obj, pd.DataFrame):
            raise TypeError(f"obj must be pd.DataFrame object, not {type(obj)}")
        handler = get_handler(path)
        if isinstance(handler, DBDataHandler):
            path = f"{path}#{table_name}"
        handler.write(path, obj, columns)
