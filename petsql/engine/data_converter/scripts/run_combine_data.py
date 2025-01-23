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

from petsql.common.spark_utils import get_saprk_session


def run_combine_data():
    params = json.loads(sys.argv[1])
    url = params["url"]
    output_path = params["output_path"]
    sqls = params["sqls"]
    output = ""
    try:
        session = get_saprk_session(url)
        tmp_insert_table = session.sql(sqls[0])
        tmp_insert_table.createOrReplaceTempView("tmp_insert_table")
        for sql in sqls[1:]:
            session.sql(sql)
        output = json.dumps({"status": "success"})
        # pylint: disable=broad-exception-caught
    except Exception as e:
        output = json.dumps({"status": "failed", "err_msg": str(e)})
    finally:
        if session:
            session.stop()
        with open(output_path, "w") as f:
            f.write(output)


if __name__ == '__main__':
    run_combine_data()
