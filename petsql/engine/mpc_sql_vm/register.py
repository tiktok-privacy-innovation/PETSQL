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

from petsql.data import PlainBigData, PlainMemoryData, MPCDuetData
from petsql.data import Schema


class RegisterItem:
    """
    RegisterItem is a class that contains the data of a register in VM.

    Parameters
    ----------
    plain_data: PlainBigData or PlainMemoryData
        The plain data.
    cipher_data: MPCDuetData
        The cipher data.
    schema: Schema
        The schema of the data.

    Attributes
    ----------
    plain_data: PlainBigData or PlainMemoryData
        The plain data.
    cipher_data: MPCDuetData
        The cipher data.
    schema: Schema
        The schema of the data.
    """

    def __init__(self, plain_data: Union[PlainBigData, PlainMemoryData], cipher_data: MPCDuetData,
                 schema: Schema) -> None:
        self.plain_data = plain_data
        self.cipher_data = cipher_data
        self.schema = schema


class Register:
    # list of RegisterItem
    data = None

    def __init__(self) -> None:
        self.data = []

    def set_data(self, data: RegisterItem):
        self.data.append(data)

    def get_data(self, index: int) -> RegisterItem:
        return self.data[index]

    def clean(self):
        self.data = []
