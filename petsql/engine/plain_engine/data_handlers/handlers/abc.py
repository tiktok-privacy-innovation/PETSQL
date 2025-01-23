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

from typing import List
from abc import ABC, abstractmethod
import pandas as pd
from petsql.data import Column


class AbstractDataHandler(ABC):
    # pylint: disable=keyword-arg-before-vararg
    @abstractmethod
    def read(self, path: str, columns: List["Column"] = None, index_column_name=None, *args, **kwargs) -> pd.DataFrame:
        pass

    # pylint: disable=keyword-arg-before-vararg
    @abstractmethod
    def write(self,
              path: str,
              data: pd.DataFrame,
              columns: List["Column"] = None,
              index_column_name=None,
              *args,
              **kwargs) -> None:
        pass
