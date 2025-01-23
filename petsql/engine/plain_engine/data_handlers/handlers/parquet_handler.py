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
from petsql.data import Column, ColumnType
from .abc import AbstractDataHandler


class ParquetDataHandler(AbstractDataHandler):

    # pylint: disable=keyword-arg-before-vararg
    def read(self, path: str, columns: List["Column"] = None, index_column_name=None, *args, **kwargs) -> pd.DataFrame:
        """
        Read parquet file and return a pandas dataframe.

        Parameters
        ----------
        path : str
            path of the parquet file.
        columns : List[str]
            _description_
        index_column_name : str
            Index column name of the data. If this name not in the columns name, it will add a index columns with index_column_name.
        *args :
            *args for parquet
        **kwargs :
            **kwargs for parquet

        Returns
        -------
        pd.DataFrame
            _description_
        """
        if columns:
            usecols = [item.name for item in columns]
            dtype = {item.name: ColumnType.to_pandas_type(item.type) for item in columns}
            ret = pd.read_parquet(path, *args, **kwargs)
            ret = ret[usecols]
            for col, col_type in dtype.items():
                ret[col] = ret[col].astype(col_type)

            if index_column_name is not None and index_column_name not in usecols:
                ret[index_column_name] = ret.index
            return ret
        return pd.read_parquet(path, *args, **kwargs)

    # pylint: disable=keyword-arg-before-vararg
    def write(self,
              path: str,
              data: pd.DataFrame,
              columns: List["Column"] = None,
              index_column_name=None,
              *args,
              **kwargs) -> None:
        """
        Write data to a parquet file.

        Parameters
        ----------
        path : str
            path of the parquet file.
        data : pd.DataFrame
            data to write.
        columns : List[str]
            columns to write.
        index_column_name : str
            Index column name of the data. If this name not in the columns name, it will add a index columns with index_column_name.
        *args :
            *args for parquet
        **kwargs :
            **kwargs for parquet
        """
        if index_column_name:
            data = data.sort_values(by=index_column_name, ascending=True)
        if columns:
            columns = [item.name for item in columns]
            data = data[columns]
            data.to_parquet(path, index=False, *args, **kwargs)
        data.to_parquet(path, index=False, *args, **kwargs)
