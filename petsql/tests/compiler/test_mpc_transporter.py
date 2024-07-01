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

from petsql.compiler import SQLCompiler
from petsql.compiler import MPCTransporter

from petsql.tests.utils import CommonTestBase, TestDataUtils
from petsql.tests.config import TestConfig


class TestMPCTransporter(CommonTestBase):

    def test_mpc_transporter(self):
        config = TestConfig.get_config()
        sql_compiler = SQLCompiler()
        sql = TestConfig.get_aimed_sql()
        logical_plan = sql_compiler.compile(sql, config)

        mpc_transporter = MPCTransporter()
        physical_plan = mpc_transporter.transport(config, logical_plan)
        aimed_physical_plan = TestDataUtils.load_json_from_file("test_physical_plan.json")
        assert aimed_physical_plan == physical_plan
