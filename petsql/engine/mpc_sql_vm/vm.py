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

from typing import Union

from petsql.common import Mode
from petsql.engine.data_converter import DataConverter
from petsql.data import PlainBigData, PlainMemoryData, MPCDuetData
from petsql.data import Schema, Party
from petsql.engine.plain_engine import PlainEngine
from petsql.engine.cipher_engine import CipherEngine

from .operator import TableScan, Join, Project
from .operator import RevealTo, Aggregate, Filter
from .register import Register, RegisterItem


class VM:

    def __init__(self,
                 party: Party,
                 cipher_impl: CipherEngine,
                 plain_impl: PlainEngine,
                 reg: Register = Register(),
                 mode: Mode = Mode.MEMORY) -> None:
        self.op_map = {}
        self.cipher_impl = cipher_impl
        self.plain_impl = plain_impl
        self.cipher_impl.set_vm(self)
        self.plain_impl.set_vm(self)
        self.reg = reg
        self.party = party
        self.mode = mode
        self.data_converter = DataConverter(cipher_impl, plain_impl)
        self._regist_default_operator()

    def regist_operator(self, k, v) -> None:
        self.op_map[k] = v

    def exec_plan(self, physical_plan) -> None:
        self.op_map[physical_plan["operatorName"]].run(self, physical_plan)

    def _regist_default_operator(self) -> None:
        self.regist_operator("TableScan", TableScan())
        self.regist_operator("Join", Join())
        self.regist_operator("Project", Project())
        self.regist_operator("RevealTo", RevealTo())
        self.regist_operator("Aggregate", Aggregate())
        self.regist_operator("Filter", Filter())

    def clean(self):
        self.reg.clean()

    def new_register_item(self, plain_data: Union[PlainBigData, PlainMemoryData], cipher_data: MPCDuetData,
                          schema: Schema):
        self.reg.set_data(RegisterItem(plain_data, cipher_data, schema))

    def get_reg_item(self, index: int) -> RegisterItem:
        return self.reg.get_data(index)
