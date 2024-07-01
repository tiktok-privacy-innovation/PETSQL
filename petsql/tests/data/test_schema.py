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

from petsql.data import Column, Schema
from petsql.data import ColumnType, Party

from petsql.tests.utils import CommonTestBase


class TestSchemaClass(CommonTestBase):

    def test_column(self):
        cloumn = Column("test_column", ColumnType.INT, Party.ZERO)
        assert cloumn.name == "test_column"
        assert cloumn.type == ColumnType.INT
        assert cloumn.party == Party.ZERO
        assert cloumn.to_dict() == {"name": "test_column", "type": 2, "party": 0}

    def test_schema(self):
        schema = Schema("test_table")
        schema.append_column(Column(name="id", type=ColumnType.INT, party=Party.ONE))
        schema.append_column(Column(name="name", type=ColumnType.CHAR, party=Party.ZERO))
        aim_dict = {
            'name': 'test_table',
            'columns': [{
                'name': 'id',
                'type': 2,
                'party': 1
            }, {
                'name': 'name',
                'type': 0,
                'party': 0
            }],
            'party': -1
        }
        assert schema.to_dict() == aim_dict
