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

from petsql.common import Config

from petsql.tests.utils import CommonTestBase


class TestConfigClass(CommonTestBase):

    def test_config(self):
        test_config_0 = Config()
        test_dict_0 = test_config_0.to_dict()

        test_config_1 = Config()
        test_config_1.from_dict(test_dict_0)
        test_dict_1 = test_config_1.to_dict()
        assert test_dict_0 == test_dict_1

        test_json_0 = test_config_0.to_json()
        test_json_1 = test_config_1.to_json()
        assert test_json_0 == test_json_1
