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

import json
from enum import Enum
from typing import Dict

from petsql.data.schema import Party, Schema


class Mode(Enum):
    MEMORY = "memory"
    BIGDATA = "bigdata"
    MIXED = "mixed"


class Config:
    """
    The config of PETSQL.

    Attributes
    ----------
    schemas : [Schema]
        A list of table schemas.
    table_url : {"table_name": url}
        The url dict of table schemas. Notice: The table_name must in upper case.
        For example: {"table_from_a": "./data/table_from_a.csv"}
    engine_url: str
        The url of compute engine.
    reveal_to : Party
        The party which get the result.
    mode : Mode
        The data mode of PETSQL.
    task_id : str
        The task id of PETSQL.
    """

    def __init__(self) -> None:
        self.schemas = []
        self.table_url = {}
        self.engine_url = ""
        self.reveal_to = Party.ZERO
        self.mode = Mode.MEMORY
        self.task_id = ""

    def to_dict(self) -> Dict:
        """
        Convert the configuration object to a dictionary.

        -------
        Returns:
        Dict
            A dictionary containing the configuration information.
        """
        ret = {
            "mode": self.mode.value,
            "schemas": [table_schema.to_dict() for table_schema in self.schemas],
            "table_url": self.table_url,
            "engine_url": self.engine_url,
            # Now only support reveal to one party
            "reveal_to": self.reveal_to.value,
            "task_id": self.task_id,
        }
        return ret

    def from_dict(self, input_dict: Dict) -> "Config":
        """
        Convert the current column object into a dictionary.

        Parameters
        ----------
        input_dict : Dict
            A dictionary containing the configuration information.

        Returns
        -------
        Config
            The configuration object.
        """
        self.schemas = []
        for item in input_dict["schemas"]:
            tmp_table_schema = Schema()
            tmp_table_schema.from_dict(item)
            self.schemas.append(tmp_table_schema)
        self.table_url = input_dict["table_url"]
        self.engine_url = input_dict["engine_url"]
        self.reveal_to = Party(input_dict["reveal_to"])
        self.mode = Mode(input_dict["mode"])
        self.task_id = input_dict["task_id"]
        return self

    def to_json(self) -> str:
        """
        Convert the configuration object to a JSON string.

        -------
        Returns:
        str
            A JSON string containing the configuration information.
        """
        return json.dumps(self.to_dict())

    def from_json(self, input_json: str) -> "Config":
        """
        Convert a JSON string to a configuration object.

        Parameters
        ----------
        input_json : str
            A JSON string containing the configuration information.

        Returns
        -------
        Config
            The configuration object.
        """
        return self.from_dict(json.loads(input_json))
