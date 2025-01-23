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
import os
import shutil

from petsql.data.schema import Column
from petsql.common.spark_utils import get_saprk_session


def run_spark_read():
    params = json.loads(sys.argv[1])
    url = params["url"]
    path = params["path"]
    table_name = params["table_name"]
    columns = params["columns"]
    columns = [Column().from_dict(item) for item in columns]
    index_column_name = params["index_column_name"]
    args = params["args"]
    kwargs = params["kwargs"]
    output_path = params["output_path"]
    output = ""
    try:
        session = get_saprk_session(url)

        sql = f"SELECT * FROM {table_name}"
        result_table = session.sql(sql)

        def write_to_one_csv(table, columns, index_columns_name, ret_path):
            df_coalesced = table.coalesce(1)
            if index_columns_name is not None:
                df_coalesced = df_coalesced.orderBy(index_columns_name)
            columns_to_select = [item.name for item in columns]
            selected_columns_df = df_coalesced.select(*columns_to_select)
            temp_dir = ret_path[0:-4]
            selected_columns_df.write.csv(temp_dir, mode="overwrite", *args, **kwargs)
            generated_file = [f for f in os.listdir(temp_dir) if f.endswith(".csv")][0]
            shutil.copy(os.path.join(temp_dir, generated_file), ret_path)

        write_to_one_csv(result_table, columns, index_column_name, path)
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
    run_spark_read()
