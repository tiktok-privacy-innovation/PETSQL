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

from petsql.tests.utils import CommonTestBase, TestDataUtils
from petsql.tests.config import TestConfig


class TestCompiler(CommonTestBase):

    def test_compiler(self):
        tmp_dir = TestConfig.get_tmp_data_path(0) + "/compiler"
        compiler = SQLCompiler(tmp_dir)
        sql = TestConfig.get_aimed_sql()
        config = TestConfig.get_config()
        ret = compiler.compile(sql, config)
        aim_ret = TestDataUtils.load_json_from_file("test_compiler_ret.json")
        assert ret == aim_ret
