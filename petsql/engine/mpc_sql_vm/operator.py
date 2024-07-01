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

from abc import ABC, abstractmethod
from typing import Dict
import copy

from petsql.data import Schema, Column, Party
from petsql.common import Mode
from petsql.data import DataType
from petsql.data import PlainMemoryData, PlainBigData
from .exception import MPCSQLOperatorInvaildParamsError


class MPCSQLOperatorBase(ABC):

    def __init__(self) -> None:
        pass

    @abstractmethod
    def run(self, vm, physical_plan):
        raise NotImplementedError


class TableScan(MPCSQLOperatorBase):

    def run(self, vm: "VM", physical_plan: Dict) -> None:
        """
        Execute the table scan operator.
        First, distinguish between plaintext and ciphertext, and then hand them over
        to the respective plain and cipher engines for execution.

        Parameters
        ----------
        vm : VM
            The virtual machine.
        physical_plan : Dict
            The physical plan.
        """

        if len(physical_plan["inputs"]["table"]) != 1:
            raise MPCSQLOperatorInvaildParamsError("TableScan", "only support one table scan.")
        output_schema = Schema().from_dict(physical_plan["outputs"][0])
        if output_schema.party == Party.SHARE:
            raise MPCSQLOperatorInvaildParamsError("TableScan", "doesn't support corss party table now")
        if output_schema.party == vm.party:
            plain_data = vm.plain_impl.table_scan(physical_plan)
            vm.new_register_item(plain_data, None, plain_data.schema)
        else:
            vm.new_register_item(None, None, output_schema)


class Join(MPCSQLOperatorBase):
    """
    Execute the join operator.
    First, distinguish between plaintext and ciphertext, and then hand them over
    to the respective plain and cipher engines for execution.

    Parameters
    ----------
    vm : VM
        The virtual machine.
    physical_plan : Dict
        The physical plan.
    """

    def run(self, vm: "VM", physical_plan: Dict):
        if len(physical_plan["inputs"]) != 2:
            raise MPCSQLOperatorInvaildParamsError("Join", "only support two table join now.")
        if physical_plan["joinType"] != "inner":
            raise MPCSQLOperatorInvaildParamsError("Join", "only support inner join now.")
        if physical_plan["condition"]["operator"] != "=":
            raise MPCSQLOperatorInvaildParamsError("Join", "only support = condition now.")
        reg_item_0 = vm.reg.get_data(int(physical_plan["inputs"][0]))
        reg_item_1 = vm.reg.get_data(int(physical_plan["inputs"][1]))
        output_schema = Schema().from_dict(physical_plan["outputs"][0])
        if Party.SHARE in (reg_item_0.schema.party, reg_item_1.schema.party):
            raise MPCSQLOperatorInvaildParamsError("Join", "unsupport shared table join now")
        if reg_item_0.schema.party == reg_item_1.schema.party:
            if reg_item_0.schema.party == vm.party:
                ret = vm.plain_impl.join([reg_item_0, reg_item_1], physical_plan)
            else:
                ret = None
        else:
            ret = vm.cipher_impl.join((reg_item_0, reg_item_1), physical_plan)
        vm.new_register_item(ret, None, output_schema)


class Project(MPCSQLOperatorBase):
    """
    Execute the project operator.
    First, distinguish between plaintext and ciphertext, and then hand them over
    to the respective plain and cipher engines for execution.

    Parameters
    ----------
    vm : VM
        The virtual machine.
    physical_plan : Dict
        The physical plan.
    """

    def run(self, vm: "VM", physical_plan: Dict):
        output_schema = Schema().from_dict(physical_plan["outputs"][0])
        reg_item = vm.reg.get_data(-1)

        def get_one_party_physical_plan(physical_plan, party):
            one_party_output_schema = Schema(name=output_schema.name, party=party)
            one_party_expressions = []
            for i in range(len(physical_plan["expressions"])):
                if output_schema.columns[i].party == party:
                    one_party_output_schema.append_column(output_schema.columns[i])
                    one_party_expressions.append(physical_plan["expressions"][i])
            return {"expressions": one_party_expressions, "outputs": [one_party_output_schema.to_dict()]}

        party_0_physical_plan = get_one_party_physical_plan(physical_plan, Party.ZERO)
        party_1_physical_plan = get_one_party_physical_plan(physical_plan, Party.ONE)
        shared_physical_plan = get_one_party_physical_plan(physical_plan, Party.SHARE)
        if vm.party == Party.ZERO:
            plain_data = vm.plain_impl.project(reg_item, party_0_physical_plan)
        else:
            plain_data = vm.plain_impl.project(reg_item, party_1_physical_plan)

        cipher_data = vm.cipher_impl.project(reg_item, shared_physical_plan)
        vm.new_register_item(plain_data, cipher_data, output_schema)


class RevealTo(MPCSQLOperatorBase):
    """
    Execute the reveal to operator.
    First, distinguish between plaintext and ciphertext, and then hand them over
    to the respective plain and cipher engines for execution.

    Parameters
    ----------
    vm : VM
        The virtual machine.
    physical_plan : Dict
        The physical plan.
    """

    def run(self, vm: "VM", physical_plan: Dict):
        reveal_to = physical_plan["reveal_to"]
        if isinstance(reveal_to, int):
            pass
        else:
            raise MPCSQLOperatorInvaildParamsError("RevealTo", "unsupport reveal to different party")
        output_schema = Schema().from_dict(physical_plan["outputs"][0])
        reg_item = vm.reg.get_data(-1)
        # reveal_share
        share_schema = Schema("_" + "_shared", Party(reveal_to), [])
        for column in reg_item.schema.columns:
            if column.party == Party.SHARE:
                tmp_column = Column(column.name, column.type, Party(reveal_to))
                share_schema.append_column(tmp_column)
        revealed_mpc_data = vm.data_converter.transport(reg_item.cipher_data, DataType.PLAIN_MEMORY, share_schema)
        # reveal_oppo
        oppo_schema = Schema("_" + "_oppo", Party(1 - reveal_to), [])
        for column in reg_item.schema.columns:
            # oppo
            if column.party.value == (1 - reveal_to):
                tmp_column = copy.deepcopy(column)
                oppo_schema.append_column(tmp_column)
        if oppo_schema.columns:
            if oppo_schema.party == vm.party:
                plain_data = reg_item.plain_data
            else:
                # pylint: disable=else-if-used
                if vm.mode == Mode.MEMORY:
                    plain_data = PlainMemoryData(None, None)
                elif vm.mode == Mode.BIGDATA:
                    plain_data = PlainBigData(None, None)
            tmp_oppo_share_data = vm.data_converter.transport(plain_data, DataType.MPC_DUET, oppo_schema)
            for column in oppo_schema.columns:
                column.party = Party(1 - column.party.value)
            oppo_data = vm.data_converter.transport(tmp_oppo_share_data, DataType.PLAIN_MEMORY, oppo_schema)
        else:
            oppo_data = PlainMemoryData(None, None)
        if reg_item.plain_data is None:
            reg_item_memory_data = None
        elif reg_item.plain_data.data_type == DataType.PLAIN_BIG_DATA:
            columns = [column.name for column in reg_item.plain_data.schema.columns]
            reg_item_memory_data_pd = vm.plain_impl.load_data_from_plain_data(reg_item.plain_data,
                                                                              reg_item.plain_data.schema.name, columns)
            reg_item_memory_data = PlainMemoryData(reg_item_memory_data_pd, reg_item.plain_data.schema)
        else:
            reg_item_memory_data = reg_item.plain_data
        vm.new_register_item(
            vm.data_converter.combine_data([reg_item_memory_data, revealed_mpc_data, oppo_data], output_schema), None,
            output_schema)


class Aggregate(MPCSQLOperatorBase):
    """
    Execute the aggregate operator.
    First, distinguish between plaintext and ciphertext, and then hand them over
    to the respective plain and cipher engines for execution.

    Parameters
    ----------
    vm : VM
        The virtual machine.
    physical_plan : Dict
        The physical plan.
    """

    def run(self, vm: "VM", physical_plan: Dict):
        if physical_plan["group"]:
            if len(physical_plan["group"]) > 1:
                raise MPCSQLOperatorInvaildParamsError("Aggregate",
                                                       "unsupport aggregate with group by more than one column")
            if physical_plan["group"][0]["party"] == Party.SHARE.value:
                raise MPCSQLOperatorInvaildParamsError("Aggregate", "unsupport aggregate with shared column")
        else:
            raise MPCSQLOperatorInvaildParamsError("Aggregate", "unsupport aggregate without group by column")

        output_schema = Schema().from_dict(physical_plan["outputs"][0])
        reg_item = vm.reg.get_data(-1)
        group_by_column_party = Party(physical_plan["group"][0]["party"])
        plain_output_schema = Schema(name=output_schema.name, party=group_by_column_party)
        share_output_schema = Schema(name=output_schema.name, party=Party.SHARE)
        plain_physical_plan = {"operatorName": "Aggregate", "group": physical_plan["group"], "aggregates": []}
        share_pyhsical_plan = copy.deepcopy(plain_physical_plan)
        plain_output_schema.append_column(output_schema.columns[0])
        for idx, aggregate in enumerate(physical_plan["aggregates"]):
            agg_column = Column().from_dict(aggregate["operands"][0])
            if agg_column.party == group_by_column_party:
                plain_physical_plan["aggregates"].append(aggregate)
                plain_output_schema.append_column(output_schema.columns[idx + 1])
            else:
                share_pyhsical_plan["aggregates"].append(aggregate)
                share_output_schema.append_column(output_schema.columns[idx + 1])
        plain_physical_plan["outputs"] = [plain_output_schema.to_dict()]
        share_pyhsical_plan["outputs"] = [share_output_schema.to_dict()]
        if group_by_column_party == vm.party:
            plain_data = vm.plain_impl.aggregate(reg_item, plain_physical_plan)
        else:
            plain_data = None
        if share_output_schema.columns:
            cipher_data = vm.cipher_impl.aggregate(reg_item, share_pyhsical_plan)
        vm.new_register_item(plain_data, cipher_data, output_schema)


class Filter(MPCSQLOperatorBase):
    """
    Execute the aggregate operator.
    Now only support plain filter operator.

    Parameters
    ----------
    vm : VM
        The virtual machine.
    physical_plan : Dict
        The physical plan.
    """

    def run(self, vm: "VM", physical_plan: Dict):
        output_schema = Schema().from_dict(physical_plan["outputs"][0])
        if output_schema.party == Party.SHARE:
            raise MPCSQLOperatorInvaildParamsError("Filter", "unsupport filter with shared column")
        reg_item = vm.reg.get_data(-1)
        if output_schema.party == vm.party:
            plain_data = vm.plain_impl.filter(reg_item, physical_plan)
        else:
            plain_data = None
        vm.new_register_item(plain_data, None, output_schema)
