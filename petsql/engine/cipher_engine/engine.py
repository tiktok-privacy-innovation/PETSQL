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

from typing import List, Union, Dict
import copy
import pandas as pd
import numpy as np

import petace.securenumpy as snp
import petace.securesql as ssql
from petsql.data import ColumnType, Schema, Column
from petsql.data import Party
from petsql.compiler import ArithOperator
from petsql.data import PlainMemoryData, MPCDuetData, DataType
from petsql.data import PlainBigData
from petsql.common import Mode
from petsql.engine.mpc_sql_vm.register import RegisterItem
from .exception import CipherEngineInvaildParamsException


class CipherEngine:
    """
    CipherEngine is a unified execution engine for PETSQL.
    It is responsible for executing the cipher physical execution plan.

    Parameters
    ----------
    duet: Duet
        The duet engine.
    psi: PSI
        The psi engine.
    mode: Mode
        Mode is a enum class that is responsible for indicating the mode of execution.

    Attributes
    ----------
    duet: Duet
        The duet engine.
    psi: PSI
        The psi engine.
    mode: Mode
        Mode is a enum class that is responsible for indicating the mode of execution.
    vm: VM
        petsql.engine.mpc_sql_vm.vm.VM.
        The vm is a class that is responsible for executing the physical execution plan.
    """

    def __init__(self, duet, psi, mode: Mode = None) -> None:
        self.duet = duet
        self.psi = psi
        self.vm = None
        self.mode = mode

    def set_vm(self, vm: "VM") -> None:
        """
        Set the virtual machine object.

        Parameters
        ----------
        vm: VM
            VM is a class that is responsible for executing the physical execution plan.
        """
        self.vm = vm

    def join(self, data: List["RegisterItem"], cipher_plan: Dict) -> Union[PlainMemoryData, PlainBigData]:
        """
        Join two tables. Now only support inner join, using ECDH protocol.

        Parameters
        ----------
        data: List["RegisterItem"]
            The data to join.
        cipher_plan: Dict
            The cipher physical plan.

        Returns
        -------
        Union[PlainMemoryData, PlainBigData]
            The output data.
        """

        output_schema = Schema().from_dict(cipher_plan["outputs"][0])

        def get_column_name_map(data, output_schema):
            column_name_map_forward = []
            column_name_map_backward = {}
            map_count = 0
            for index, item in enumerate(data):
                column_name_map_forward.append({})
                for column in item.schema.columns:
                    column_name_map_backward[output_schema.columns[map_count].name] = (column, index)
                    column_name_map_forward[index][column.name] = output_schema.columns[map_count].name
                    map_count += 1
            return column_name_map_forward, column_name_map_backward

        column_name_map_forward, column_name_map_backward = get_column_name_map(data, output_schema)

        column_to_join_0, table_index_0 = column_name_map_backward[cipher_plan["condition"]["operands"][0]["input"]
                                                                   ["name"]]
        column_to_join_1, table_index_1 = column_name_map_backward[cipher_plan["condition"]["operands"][1]["input"]
                                                                   ["name"]]
        if data[table_index_0].schema.party.value == self.duet.party_id():
            data_to_trans = data[table_index_0].plain_data
            data_to_trans_schema = data[table_index_0].plain_data.schema
            column_to_join = copy.deepcopy(column_to_join_0)
            column_name_map_forward = column_name_map_forward[table_index_0]
        else:
            data_to_trans = data[table_index_1].plain_data
            data_to_trans_schema = data[table_index_1].plain_data.schema
            column_to_join = copy.deepcopy(column_to_join_1)
            column_name_map_forward = column_name_map_forward[table_index_1]
        data_to_join = self.vm.plain_impl.load_data_from_plain_data(data_to_trans, data_to_trans_schema.name,
                                                                    [column_to_join.name])
        data_to_join = data_to_join[column_to_join.name].astype(str).tolist()

        psi_ret = self.psi.process(data_to_join, True)

        # trans psi ret to data
        psi_ret_column = copy.deepcopy(column_to_join)
        psi_ret_column.name = column_to_join.name + "_psi_ret".upper()
        psi_ret_df = pd.DataFrame(psi_ret, columns=[psi_ret_column.name])
        psi_ret_df = psi_ret_df.astype(ColumnType.to_python_type(psi_ret_column.type))
        psi_ret_schema = Schema(output_schema.name + "_psi_ret", psi_ret_column.party, [psi_ret_column])
        if self.mode == Mode.MEMORY:
            psi_ret_data = PlainMemoryData(psi_ret_df, psi_ret_schema)
        elif self.mode == Mode.BIGDATA:
            url = data_to_trans.data
            self.vm.plain_impl.save_data(url, psi_ret_df, psi_ret_schema.name, [psi_ret_column.name])
            psi_ret_data = PlainBigData(url, psi_ret_schema)
        else:
            raise CipherEngineInvaildParamsException("Join", f"Not support mode: {self.mode}")
        join_ret_schema = Schema(output_schema.name, psi_ret_column.party, [])
        for column in data_to_trans_schema.columns:
            tmp_column = Column(column_name_map_forward[column.name], column.type, column.party)
            join_ret_schema.columns.append(tmp_column)
        join_ret_schema.columns.append(psi_ret_column)
        # make plain physical plan
        column_to_join.name = column_name_map_forward[column_to_join.name]
        psi_ret_to_join_ret_plan = {
            "operatorName": "Join",
            "condition": {
                "operator": "=",
                "operands": [{
                    "input": column_to_join.to_dict()
                }, {
                    "input": psi_ret_column.to_dict()
                }]
            },
            "outputs": [join_ret_schema.to_dict()]
        }

        data_to_trans_item = RegisterItem(data_to_trans, None, data_to_trans_schema)
        psi_ret_to_join_ret_item = RegisterItem(psi_ret_data, None, psi_ret_schema)
        ret = self.vm.plain_impl.join([data_to_trans_item, psi_ret_to_join_ret_item], psi_ret_to_join_ret_plan)
        return ret

    def project(self, data: "RegisterItem", cipher_plan: Dict) -> MPCDuetData:
        """
        Project the data.

        Parameters
        ----------
        data: List["RegisterItem"]
            The data to project.
        cipher_plan: Dict
            The cipher physical plan.

        Returns
        -------
        MPCDuetData
            The output data.

        """
        output_schema = Schema().from_dict(cipher_plan["outputs"][0])

        def _get_expr_ret(expression, mpc_impl, data):
            if "literal" in expression:
                return float(expression["literal"])
            if "input" in expression:
                tmp_column = Column().from_dict(expression["input"])
                tmp_schema = Schema("_", tmp_column.party, [tmp_column])
                if tmp_column.party == Party.SHARE:
                    return self.vm.data_converter.transport(data.cipher_data, DataType.MPC_DUET,
                                                            tmp_schema).data[tmp_column.name]
                if tmp_column.party.value == self.duet.party_id():
                    plain_data = data.plain_data
                else:
                    # pylint: disable=else-if-used
                    if self.vm.mode == Mode.MEMORY:
                        plain_data = PlainMemoryData(None, None)
                    elif self.vm.mode == Mode.BIGDATA:
                        plain_data = PlainBigData(None, None)
                return self.vm.data_converter.transport(plain_data, DataType.MPC_DUET, tmp_schema).data[tmp_column.name]
            if "operator" in expression:
                operator = expression["operator"]
                if not ArithOperator.is_airth_op(operator):
                    raise CipherEngineInvaildParamsException("Project", f"unsupport op name: {operator}")
                first = _get_expr_ret(expression["operands"][0], mpc_impl, data)
                second = _get_expr_ret(expression["operands"][1], mpc_impl, data)
                return ArithOperator.exec(first, second, operator)
            raise CipherEngineInvaildParamsException("Project", f"unkown exprs: {expression}")

        duet_data_tmp = {}
        for index, expression in enumerate(cipher_plan["expressions"]):
            ret = _get_expr_ret(expression, self.duet, data)
            duet_data_tmp[output_schema.columns[index].name] = ret
        return MPCDuetData(duet_data_tmp, output_schema)

    def aggregate(self, data: "RegisterItem", cipher_plan: Dict) -> MPCDuetData:
        """
        Aggregate the data.

        Parameters
        ----------
        data: List["RegisterItem"]
            The data to aggregate.
        cipher_plan: Dict
            The cipher physical plan.

        Returns
        -------
        MPCDuetData
            The output data.

        """

        output_schema = Schema().from_dict(cipher_plan["outputs"][0])
        group_by_column = Column().from_dict(cipher_plan["group"][0])
        if self.duet.party_id() == group_by_column.party.value:
            group_by_column_data = self.vm.plain_impl.load_data_from_plain_data(data.plain_data,
                                                                                data.plain_data.schema.name,
                                                                                [group_by_column.name])
            group_by_column_data = pd.get_dummies(group_by_column_data, columns=[group_by_column.name]).astype(int)

            group_by_column_data = group_by_column_data.to_numpy().astype(np.float64)
        else:
            group_by_column_data = None

        group_by_column_array = snp.array(group_by_column_data, group_by_column.party.value)
        tmp_ret = {}
        for idx, aggregate in enumerate(cipher_plan["aggregates"]):
            agg_column = Column().from_dict(aggregate["operands"][0])
            if agg_column.party == Party.SHARE:
                agg_column_array = data.cipher_data.data[agg_column.name]
            if agg_column.party.value == (1 - group_by_column.party.value):
                tmp_agg_schema = Schema("_", agg_column.party, [agg_column])
                if self.duet.party_id() == group_by_column.party.value:
                    if self.vm.mode == Mode.MEMORY:
                        agg_column_data = PlainMemoryData(None, None)
                    elif self.vm.mode == Mode.BIGDATA:
                        agg_column_data = PlainBigData(None, None)
                else:
                    agg_column_data = data.plain_data

                agg_column_array = self.vm.data_converter.transport(agg_column_data, DataType.MPC_DUET,
                                                                    tmp_agg_schema).data[agg_column.name]

            if aggregate["aggregate"]["name"] == "SUM":
                tmp_ret[output_schema.columns[idx].name] = ssql.groupby_sum(agg_column_array,
                                                                            group_by_column_array).transpose()
            if aggregate["aggregate"]["name"] == "MAX":
                tmp_ret[output_schema.columns[idx].name] = ssql.groupby_max(agg_column_array,
                                                                            group_by_column_array).transpose()
            if aggregate["aggregate"]["name"] == "MIN":
                tmp_ret[output_schema.columns[idx].name] = ssql.groupby_min(agg_column_array,
                                                                            group_by_column_array).transpose()
        return MPCDuetData(tmp_ret, output_schema)
