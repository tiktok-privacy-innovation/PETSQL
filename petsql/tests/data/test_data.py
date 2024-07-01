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

from petsql.data import PlainBigData, PlainMemoryData, MPCDuetData
from petsql.data import DataType

from petsql.tests.utils import CommonTestBase


class TestDataClass(CommonTestBase):

    def test_data(self):
        plain_memory_data = PlainMemoryData(None, None)
        assert plain_memory_data.data_type == DataType.PLAIN_MEMORY

        plain_big_data = PlainBigData(None, None)
        assert plain_big_data.data_type == DataType.PLAIN_BIG_DATA

        mpc_duet_data = MPCDuetData(None, None)
        assert mpc_duet_data.data_type == DataType.MPC_DUET
