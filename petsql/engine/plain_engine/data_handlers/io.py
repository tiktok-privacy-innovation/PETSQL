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

import os
import uuid
import subprocess
import json
from typing import List
import pandas as pd
from petsql.data.schema import Column
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

    # pylint: disable=keyword-arg-before-vararg
    @staticmethod
    def read(path: str, table_name: str, columns: List = None, index_column_name=None, *args, **kwargs) -> pd.DataFrame:
        """read data from path, returns PlainData object

        Parameters
        ----------
        path : str
            data path.
        table_name : str
            table name.
        columns : List, optional
            Columns to write.
        index_column_name : str
            Index column name of the data. If this name not in the columns name, it will add a index columns with index_column_name.
        *args :
            *args for handler
        **kwargs :
            **kwargs for handler

        Returns
        -------
        pd.DataFrame
        """
        handler = get_handler(path)
        if isinstance(handler, DBDataHandler):
            path = f"{path}#{table_name}"
        df = handler.read(path, columns, index_column_name, *args, **kwargs)
        return df

    # pylint: disable=keyword-arg-before-vararg
    @staticmethod
    def write(path: str,
              obj: pd.DataFrame,
              table_name: str,
              columns=None,
              index_column_name=None,
              *args,
              **kwargs) -> None:
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
        index_column_name : str
            Index column name of the data. If this name not in the columns name, it will add a index columns with index_column_name.
        *args :
            *args for handler
        **kwargs :
            **kwargs for handler
        """
        if not isinstance(obj, pd.DataFrame):
            raise TypeError(f"obj must be pd.DataFrame object, not {type(obj)}")
        handler = get_handler(path)
        if isinstance(handler, DBDataHandler):
            path = f"{path}#{table_name}"
        handler.write(path, obj, columns, index_column_name, *args, **kwargs)


class SparkDataHandler:

    @staticmethod
    def read(url, path: str, table_name, columns: List["Column"], index_column_name, *args, **kwargs):
        json_data = json.loads(url.split("///")[1])
        tmp_path = json_data["warehouse_dir"]
        os.makedirs(tmp_path, exist_ok=True)
        parmas = {
            "url": url,
            "path": path,
            "table_name": table_name,
            "columns": [column.to_dict() for column in columns],
            "index_column_name": index_column_name,
            "args": args,
            "kwargs": kwargs,
            "output_path": tmp_path + f"/spark_read_output_{uuid.uuid4()}.json"
        }
        # pylint: disable=too-many-try-statements
        try:
            script_path = os.path.dirname(os.path.abspath(__file__)) + "/scripts/run_spark_read.py"
            subprocess.run(["python3", script_path, json.dumps(parmas)])
            if os.path.exists(parmas["output_path"]):
                with open(parmas["output_path"], "r") as f:
                    ret = json.loads(f.read())
                if ret["status"] == "success":
                    return None
                raise RuntimeError(ret["err_msg"])
            raise RuntimeError("spark hanlder read subprocess error")
        finally:
            if os.path.exists(parmas["output_path"]):
                os.remove(parmas["output_path"])

    @staticmethod
    def write(url, data: str, table_name: str, columns: List["Column"], index_column_name, *args, **kwargs):
        json_data = json.loads(url.split("///")[1])
        tmp_path = json_data["warehouse_dir"]
        os.makedirs(tmp_path, exist_ok=True)
        parmas = {
            "url": url,
            "data": data,
            "table_name": table_name,
            "columns": [column.to_dict() for column in columns],
            "index_column_name": index_column_name,
            "args": args,
            "kwargs": kwargs,
            "output_path": tmp_path + f"/spark_write_output_{uuid.uuid4()}.json"
        }
        # pylint: disable=too-many-try-statements
        try:
            script_path = os.path.dirname(os.path.abspath(__file__)) + "/scripts/run_spark_write.py"
            subprocess.run(["python3", script_path, json.dumps(parmas)])
            if os.path.exists(parmas["output_path"]):
                with open(parmas["output_path"], "r") as f:
                    ret = json.loads(f.read())
                if ret["status"] == "success":
                    return None
                raise RuntimeError(ret["err_msg"])
            raise RuntimeError("spark hanlder write subprocess error")
        finally:
            if os.path.exists(parmas["output_path"]):
                os.remove(parmas["output_path"])
