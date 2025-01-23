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
import json
import uuid
import os
import subprocess

from .abc import AbstractSqlDriver


class SparkDriver(AbstractSqlDriver):
    url = None

    def connect(self, url: str):
        self.url = url
        json_data = json.loads(url.split("///")[1])
        self.tmp_path = json_data["warehouse_dir"]
        os.makedirs(self.tmp_path, exist_ok=True)

    def execute(self, sql: str, show=False):
        return self.execute_batch([sql], show)

    def execute_batch(self, sqls: List[str], show=False):
        parmas = {
            "sqls": sqls,
            "url": self.url,
            "output_path": self.tmp_path + f"/spark_output_{uuid.uuid4()}.json",
            "show": show
        }
        # pylint: disable=too-many-try-statements
        try:
            script_path = os.path.dirname(os.path.abspath(__file__)) + "/scripts/run_spark_sql.py"
            subprocess.run(["python3", script_path, json.dumps(parmas)])
            if os.path.exists(parmas["output_path"]):
                with open(parmas["output_path"], "r") as f:
                    ret = json.loads(f.read())
                if ret["status"] == "success":
                    return None
                raise RuntimeError(ret["err_msg"])
            raise RuntimeError("sql driver subprocess error")
        finally:
            if os.path.exists(parmas["output_path"]):
                os.remove(parmas["output_path"])
