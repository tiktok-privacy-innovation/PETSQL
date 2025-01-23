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

import json
import sys

from jpype import JClass, getDefaultJVMPath, startJVM
from jpype import shutdownJVM, isJVMStarted
from petsql.common import Config


def java_compiler():
    params = json.loads(sys.argv[1])
    java_path = params["java_path"]
    sql = params["sql"]
    config = Config().from_dict(params["config"])
    output_path = params["output_path"]
    output = ""
    try:
        startJVM(getDefaultJVMPath(), "-ea", "-Djava.class.path=" + java_path)
        sql_compiler = JClass("com.tiktok.petsql.SQLCompiler")
        compiler = sql_compiler()
        tmp = config.to_dict()
        ret = compiler.sqlToLogicPlan(sql, json.dumps({"schemas": tmp["schemas"]}))
        output = json.dumps({"status": "success", "ret": json.loads(str(ret))["rels"]})
    # pylint: disable=broad-exception-caught
    except Exception as e:
        output = json.dumps({"status": "failed", "err_msg": str(e)})
    finally:
        if isJVMStarted():
            shutdownJVM()
        with open(output_path, "w") as f:
            f.write(output)


if __name__ == '__main__':
    java_compiler()
