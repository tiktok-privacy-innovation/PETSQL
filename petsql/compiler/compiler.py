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
import json
import subprocess
import uuid
from typing import List

from petsql.common import Config

from .exception import SQLCompilerException


class SQLCompiler:
    """
    SQLCompiler is a class that compiles SQL statements into logical plans.
    The class invokes Calcite.

    Parameters
    ----------
    java_path : str, default=None
        The path of the Java class file. If not provided, the default path is used.

    Attributes
    ----------
    java_path : str
        The path of the Java class file.
    """

    def __init__(self, tmp_path, java_path=None) -> None:
        self.tmp_path = tmp_path
        os.makedirs(tmp_path, exist_ok=True)
        if java_path is None:
            self.java_path = os.path.dirname(os.path.abspath(__file__)) + "/__binding/petsql-1.0.jar"

    def compile(self, sql: str, config: Config) -> List:
        """
        Compile the SQL statement into a logical plan.

        Parameters
        ----------
        sql : str
            The SQL statement to be compiled.
        config : Config
            The configuration of the SQL statement.

        Returns
        -------
        List
            The logical plan of the SQL statement.
        """
        parmas = {
            "sql": sql,
            "config": config.to_dict(),
            "java_path": self.java_path,
            "output_path": self.tmp_path + f"/compiler_output_{uuid.uuid4()}.json"
        }
        # pylint: disable=too-many-try-statements
        try:
            script_path = os.path.dirname(os.path.abspath(__file__)) + "/scripts/java_compiler.py"
            subprocess.run(["python3", script_path, json.dumps(parmas)])
            if os.path.exists(parmas["output_path"]):
                with open(parmas["output_path"], "r") as f:
                    ret = json.loads(f.read())
                if ret["status"] == "success":
                    return ret["ret"]
                raise SQLCompilerException(ret["err_msg"])
            raise SQLCompilerException("subprocess error")
        finally:
            if os.path.exists(parmas["output_path"]):
                os.remove(parmas["output_path"])
