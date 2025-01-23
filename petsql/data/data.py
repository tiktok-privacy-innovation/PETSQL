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

from enum import Enum
from typing import Dict

import pandas as pd

from petace.securenumpy import SecureArray

from .schema import Schema


class DataType(Enum):
    MPC_DUET = 0
    PLAIN_MEMORY = 1
    PLAIN_BIG_DATA = 2
    PLAIN_SPARK = 3
    MPC_BIGDATA = 4


class Data:
    """
    Unified Data Object for Data Layer

    Parameters
    ----------
    data : any
        "Pointer" to Actual Data.
    data_type : DataType
        Type of data.
    schema : Schema
        Schema of data.

    Attributes
    ----------
    data : any
        "Pointer" to Actual Data.
    data_type : DataType
        Type of data.
    schema : Schema
        Schema of data.
    """

    def __init__(self, data, data_type: "DataType", schema: "Schema", index: str = None) -> None:
        self.data = data
        self.data_type = data_type
        self.schema = schema
        self.index = index


class PlainMemoryData(Data):
    """
    Plain memory data instance.

    Parameters
    ----------
    data : pd.DataFrame
        Plain memory data.
    schema : Schema
        Schema of data.

    Attributes
    ----------
    data : pd.DataFrame
        Plain memory data.
    data_type : DataType.PLAIN_MEMORY
        Type of data. (Plain Memory Data)
    schema : Schema
        Schema of data.
    """

    def __init__(self, data: pd.DataFrame, schema: "Schema", index: str = None) -> None:
        super().__init__(data, DataType.PLAIN_MEMORY, schema, index)


class PlainBigData(Data):
    """
    Plain big data instance.

    Parameters
    ----------
    data : str
        The URL for the data.
    schema : Schema
        Schema of data.

    Attributes
    ----------
    data : str
        The URL for the data.
    data_type : DataType.PLAIN_BIG_DATA
        Type of data. (Plain Big Data)
    schema : Schema
        Schema of data.
    """

    def __init__(self, data: str, schema: "Schema", index: str = None) -> None:
        super().__init__(data, DataType.PLAIN_BIG_DATA, schema, index)


class MPCDuetData(Data):
    """
    MPC duet data instance.

    Parameters
    ----------
    data : SecureArray
        Cipher data which is in the SecureArray form.
    schema : Schema
        Schema of data.

    Attributes
    ----------
    data : SecureArray
        Cipher data which is in the SecureArray form.
    data_type : DataType.MPC_DUET
        Type of data. (MPC Duet Data)
    schema : Schema
        Schema of data.
    """

    def __init__(self, data: Dict[str, SecureArray], schema: "Schema", index: str = None) -> None:
        super().__init__(data, DataType.MPC_DUET, schema, index)


class PlainSparkData(Data):
    """
    Plain spark data instance.

    Parameters
    ----------
    data : str
        The URL for the data.
    schema : Schema
        Schema of data.

    Attributes
    ----------
    data : str
        The URL for the data.
    data_type : DataType.PLAIN_BIG_DATA
        Type of data. (Plain Big Data)
    schema : Schema
        Schema of data.
    """

    def __init__(self, data: str, schema: "Schema", index: str = None) -> None:
        super().__init__(data, DataType.PLAIN_SPARK, schema, index)
        self.index = index


class MPCBigdataData(Data):
    """
    MPC bigdata data instance.

    Parameters
    ----------
    data : SecureArray
        Cipher data which is in the SecureArray form.
    schema : Schema
        Schema of data.

    Attributes
    ----------
    data : SecureArray
        Cipher data which is in the SecureArray form.
    data_type : DataType.MPC_DUET
        Type of data. (MPC Duet Data)
    schema : Schema
        Schema of data.
    """

    def __init__(self, data: Dict[str, SecureArray], schema: "Schema", index: str = None) -> None:
        super().__init__(data, DataType.MPC_BIGDATA, schema, index)
