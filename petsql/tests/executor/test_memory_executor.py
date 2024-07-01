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

from petsql.tests.utils import PETSQLTestBase, TestDataUtils
from petsql.tests.config import TestConfig

from petsql.common import Mode


class TestMemoryExecutor(PETSQLTestBase):

    def run_process(self, party):
        self._init_executor(party, Mode.MEMORY)
        return super().run_process(party)

    def test_execute_sql(self, party):
        sql = TestConfig.get_aimed_sql()
        config = TestConfig.get_config()
        ret = self.executor.exec_sql(sql, config)
        if party == 0:
            aim_ret = TestDataUtils.get_sql_result(config, sql)
            assert ((ret.plain_data.data - aim_ret).abs() < 1).all().all()
