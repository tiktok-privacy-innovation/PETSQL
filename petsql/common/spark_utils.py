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
from pyspark.sql import SparkSession


def get_saprk_session(url: str):
    spark_config = json.loads(url.split("///")[1])
    app_name = spark_config["app_name"]
    master = spark_config["master"]
    warehouse_dir = spark_config["warehouse_dir"]
    metastore_db_dir = spark_config["metastore_db_dir"]

    session = SparkSession.builder \
    .appName(app_name) \
    .master(master) \
    .config("spark.sql.warehouse.dir", warehouse_dir) \
    .config("javax.jdo.option.ConnectionURL", f"jdbc:derby:;databaseName={metastore_db_dir};create=true") \
    .enableHiveSupport() \
    .getOrCreate()
    return session
