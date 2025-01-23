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

import traceback
from petsql.tests.utils import PETSQLTestBase, TestDataUtils
from petsql.tests.config import TestConfig
from petsql.engine.plain_engine.sql_engine import SqlEngineFactory

from petsql.common import Mode


class TestMemoryExecutor(PETSQLTestBase):

    def run_process(self, party):
        self._init_spark_executor(party, Mode.SPARK)
        return super().run_process(party)

    def test_execute_sql(self, party):
        sql = TestConfig.get_aimed_sql()
        config = TestConfig.get_spark_config(party)
        try:
            ret = self.executor.exec_sql(sql, config)
        except Exception as e:
            traceback.print_exc()
            raise e
        if party == 0:
            sql_engine = SqlEngineFactory.create_engine(config.engine_url)
            sql_engine.execute(f"select * from {ret.plain_data.schema.name}", True)
        if party == 0:
            aim_ret = TestDataUtils.get_sql_result(config, sql)
            print(aim_ret)
