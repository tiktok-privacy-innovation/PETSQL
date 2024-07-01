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

import copy
from abc import ABC, abstractmethod
from typing import Dict, List, Tuple

from petace.securenumpy import SecureArray

from petsql.common import Config
from petsql.data import Schema, Column
from petsql.data import ColumnType, Party

from .exception import MPCSQLTransporterException


class MPCTransporter:
    """
    MPCTransporter is a class that transports the logical plan to the physical plan.
    It introduces the concept of parties and related derivations based on the characteristics
    of MPC (Multi-Party Computation).
    The transformation focuses on SQL operators, and currently supports operators LogicalTableScan,
    LogicalProject, LogicalJoin, LogicalFilter, and LogicalAggregate.

    Attributes
    ----------
    operator : Dict
        The dictionary of operators.
    """

    def __init__(self) -> None:
        self._register()

    def _register(self) -> None:
        """
        Register operators.
        Currently supports operators LogicalTableScan, LogicalProject,
        LogicalJoin, LogicalFilter, and LogicalAggregate.
        """
        self.operator = {}
        self.operator["LogicalTableScan"] = TableScan()
        self.operator["LogicalProject"] = Project()
        self.operator["LogicalJoin"] = Join()
        self.operator["LogicalFilter"] = Filter()
        self.operator["LogicalAggregate"] = Aggregate()

    def transport(self, config: "Config", logic_plan: List):
        """
        Transport the given logic plan to physical plan.

        Parameters
        ----------
        config : Config
            The configuration of the PETSQL.
        logic_plan : List
            The logical plan.

        Returns
        -------
        List
            The physical plan.

        Raises
        ------
        MPCSQLTransporterException
            If the operator is not supported.
        """
        context = MPCTransporterContext(config)
        ret = []
        for item in logic_plan:
            op_name = item["relOp"]
            operator = self.operator.get(op_name)
            if operator is None:
                raise MPCSQLTransporterException(f"unknown operator: {op_name}")
            ret.append(operator.transport(context, item))
        ret.append(RevealTo().transport(context))
        return ret


class MPCOperatorTransporterBase(ABC):
    """
    MPCOperatorTransporterBase is an abstract class that defines the interface for
    operator transporters.
    """

    @abstractmethod
    def transport(self, context: "MPCTransporterContext", logic_operator: Dict) -> Dict:
        """
        Transport the given logic operator to physical operator.

        Parameters
        ----------
        context : MPCTransporterContext
            The context of the transporter.
        logic_operator : Dict
            The logic operator.

        Returns
        -------
        Dict
            The physical operator.

        Raises
        ------
        MPCSQLTransporterException
            If the operator is not supported.
        """
        raise NotImplementedError

    def _input_to_column(self, last_schema: "Schema", expression: Dict) -> Dict:
        """
        Convert the given expression to column.

        Parameters
        ----------
        last_schema : Schema
            The last table schema.
        expression : Dict
            The expression to convert.

        Returns
        -------
        Dict
            The converted expression.
        """
        return {"input": last_schema.columns[expression["input"]].to_dict()}

    def _get_literal_type(self, type_str: str) -> ColumnType:
        """
        Convert a string type to a column type.

        Parameters
        ----------
        type_str : str
            The string type to convert.

        Returns
        -------
        ColumnType
            The corresponding column type.

        Raises
        ------
        MPCSQLTransporterException
            If the string type is unknown.
        """
        if type_str == "INTEGER":
            return ColumnType.INT
        if type_str == "DECIMAL":
            return ColumnType.DOUBLE
        raise MPCSQLTransporterException(f"unknown type: {type_str}")

    def _get_expression(self, last_schema: "Schema", expression: Dict) -> Tuple["Party", "ColumnType", Dict]:
        """
        Parse and process expressions.

        Parameters
        ----------
        last_schema : Schema
            The last table in the schema.
        expression : Dict
            The expression to process.

        Returns
        -------
        Tuple["Party", "ColumnType", Dict]
            A tuple containing the party, type, and processed expression.

        Raises
        ------
        MPCSQLTransporterException
            If the expression is unknown or unsupported.
        """
        if "input" in expression:
            last_column = last_schema.columns[expression["input"]]
            return last_column.party, last_column.type, self._input_to_column(last_schema, expression)
        if "op" in expression:
            op_name = expression["op"]["name"]
            if not ArithOperator.is_airth_op(op_name) and not BooleanOperator.is_boolean_op(op_name):
                raise MPCSQLTransporterException(f"unsupport op: {op_name}")
            first_party, first_type, first_expression = self._get_expression(last_schema, expression["operands"][0])
            second_party, second_type, second_expression = self._get_expression(last_schema, expression["operands"][1])

            def get_ret_party(first_party: "Party", second_party: "Party") -> "Party":
                if first_party == Party.PUBLIC:
                    return second_party
                if second_party == Party.PUBLIC:
                    return first_party
                if first_party == second_party:
                    return first_party
                return Party.SHARE

            def get_ret_type(op_name: str, first_type: "ColumnType", second_type: "ColumnType") -> "ColumnType":
                # now only support int and float
                if BooleanOperator.is_boolean_op(op_name):
                    return ColumnType.BOOLEAN
                return ColumnType(max(first_type.value, second_type.value))

            expression_now = {"operator": op_name, "operands": [first_expression, second_expression]}
            return get_ret_party(first_party, second_party), get_ret_type(op_name, first_type,
                                                                          second_type), expression_now
        if "literal" in expression:
            expression_now = {"literal": expression["literal"]}
            return Party.PUBLIC, self._get_literal_type(expression["type"]["type"]), expression_now
        raise MPCSQLTransporterException(f"unknown how to trans: {expression}")


class ArithOperator:

    @staticmethod
    def is_airth_op(op: str) -> bool:
        """
        Check if the given string is an arithmetic operator.

        Parameters
        ----------
        op : str
            The string to check.

        Returns
        -------
        bool
            True if the string is an arithmetic operator, False otherwise.
        """
        if op in set(["+", "-", "*", "/"]):
            return True
        return False

    @staticmethod
    def exec(a: "SecureArray", b: "SecureArray", op_name: str) -> "SecureArray":
        """
        Perform arithmetic operations on two SecureArray objects.

        Parameters
        ----------
        a : SecureArray
            The first operand.
        b : SecureArray
            The second operand.
        op_name : str
            The name of the operation to perform.

        Returns
        -------
        SecureArray
            The result of the operation.

        Raises
        ------
        ValueError
            If the operation name is not supported.
        """
        operations = {
            "+": a + b,
            "-": a - b,
            "*": a * b,
            "/": a / b,
        }

        if op_name in operations:
            return operations[op_name]
        raise ValueError(f"Unsupported operation name: {op_name}")


class BooleanOperator:

    @staticmethod
    def is_boolean_op(op: str) -> bool:
        """
        Check if the given string is a boolean operator.

        Parameters
        ----------
        op : str
            The string to check.

        Returns
        -------
        bool
            True if the string is a boolean operator, False otherwise.
        """
        if op in set(["==", "=", "!=", ">", "<", ">=", "<="]):
            return True
        return False

    @staticmethod
    def exec(a: "SecureArray", b: "SecureArray", op_name: str) -> "SecureArray":
        """
        Perform comparison operations on two SecureArray objects.

        Parameters
        ----------
        a : SecureArray
            The first operand.
        b : SecureArray
            The second operand.
        op_name : str
            The name of the operation to perform.

        Returns
        -------
        SecureArray
            The result of the operation.

        Raises
        ------
        ValueError
            If the operation name is not supported.
        """
        ops = {"==": a == b, "=": a == b, "!=": a != b, ">": a > b, "<": a < b, ">=": a >= b, "<=": a <= b}
        if op_name in ops:
            return ops[op_name]
        raise ValueError(f"Unsupported operation: {op_name}")


class MPCTransporterContext:
    """
    MPCTransporterContext is a class that provides context for the MPCTransporter.
    It contains the configuration of the PETSQL and the output schema of the physical plan.

    Parameters
    ----------
    config : Config
        The configuration of the PETSQL.

    Attributes
    ----------
    config : Config
        The configuration of the PETSQL.
    outputs : List
        The output schema of the physical plan.
    """

    def __init__(self, config: "Config") -> None:
        self.config = config
        self.outputs = []

    def to_dict(self):
        """
        Convert the MPCTransporterContext object to a dictionary.

        Returns
        -------
        Dict
            The dictionary representation of the MPCTransporterContext object.
        """
        outputs_tmp = []
        for item in self.outputs:
            outputs_tmp.append(item.to_dict())
        return {"config": self.config.to_dict(), "output": outputs_tmp}

    def get_all_table_name(self) -> List[str]:
        """
        Get all table names in the configuration.

        Returns
        -------
        List of str
            A List of table names.
        """
        return [item.name for item in self.config.schemas]

    def get_table_by_name(self, name: str) -> "Schema":
        """
        Get a table by its name.

        Parameters
        ----------
        name : str
            The name of the table.

        Returns
        -------
        Schema or None
            The table if found, otherwise None.
        """
        for item in self.config.schemas:
            if item.name == name:
                return item
        return None


class TableScan(MPCOperatorTransporterBase):
    """
    In the TableScan operation, aside from the generic rewriting of names,
    the primary action is to read the schema information and url information
    for the corresponding table from the config, based on the table name,
    and store this information in the outputs and inputs field.
    """

    def transport(self, context: "MPCTransporterContext", logic_operator: Dict):
        if len(logic_operator["table"]) > 1:
            raise MPCSQLTransporterException("Unsupport scaning table more than one.")
        if len(logic_operator["inputs"]) != 0:
            raise MPCSQLTransporterException("Unknown logical plan: TableScan but inputs is set.")
        op_id = logic_operator["id"]
        ret = {"id": op_id, "operatorName": "TableScan"}
        table_name_all = context.get_all_table_name()
        table_name = logic_operator["table"][0]
        if table_name not in table_name_all:
            raise MPCSQLTransporterException(f"Unknown table name: {table_name}.")
        table = context.get_table_by_name(table_name)
        outputs_table = copy.deepcopy(table)
        outputs_table.name = f"{context.config.task_id}_{op_id}"

        ret = {
            "id": op_id,
            "operatorName": "TableScan",
            "inputs": {
                "table": [table.to_dict()]
            },
            "outputs": [outputs_table.to_dict()]
        }
        ret["inputs"]["url"] = {table_name: context.config.table_url[table_name]}
        context.outputs.append([outputs_table])
        return ret


class Project(MPCOperatorTransporterBase):
    """
    The Project operation mainly performs type inference and party deduction.
    These deductions are carried out according to the rules specified for MPC.
    """

    def transport(self, context: "MPCTransporterContext", logic_operator: Dict):
        op_id = logic_operator["id"]
        last_schema = context.outputs[int(op_id) - 1][0]
        ret = {"id": op_id, "operatorName": "Project"}
        tmp_expressions = []
        table_now = Schema(f"{context.config.task_id}_{op_id}", last_schema.party)
        for i, item in enumerate(logic_operator["exprs"]):
            column_name = logic_operator["fields"][i].replace("$", "")
            column_party, column_type, expression = self._get_expression(last_schema, item)
            column_now = Column(column_name, column_type, column_party)
            table_now.append_column(column_now)
            tmp_expressions.append(expression)
        ret = {"id": op_id, "operatorName": "Project", "expressions": tmp_expressions, "outputs": [table_now.to_dict()]}
        context.outputs.append([table_now])
        return ret


class Join(MPCOperatorTransporterBase):
    """
    The Join operation involves two main modifications. Firstly, since we
    utilize ECDH PSI (Elliptic Curve Diffie-Hellman Private Set Intersection),
    the tables resulting from the join are presently marked as shared and belong
    to two parties. As we support more protocols in the future, additional
    configuration options will be added to accommodate different protocols.

    Furthermore, according to Calcite's rules, if the tables being joined have
    columns with the same name, a numeral suffix will be added to the columns of
    table "b" starting with 0. If there are still duplicate column names,
    the suffix will be incremented by 1, and this will continue until there
    are no more conflicts. We have inherited this rule.
    """

    def transport(self, context: "MPCTransporterContext", logic_operator):
        inputs = logic_operator["inputs"]
        op_id = logic_operator["id"]
        if len(inputs) != 2:
            raise MPCSQLTransporterException("now only support two table join.")
        last_schemas = []
        for table_id in inputs:
            last_schemas.append(context.outputs[int(table_id)][0])
        table_now = Schema(f"{context.config.task_id}_{op_id}", last_schemas[0].party)
        name_now = []
        for table in last_schemas:
            for column in table.columns:
                column = copy.deepcopy(column)
                name_change_count = 0
                while column.name in name_now:
                    column.name = f"{column.name}{str(name_change_count)}"
                    name_change_count += 1
                    if name_change_count > 10000:
                        raise MPCSQLTransporterException("name change count too much.")
                name_now.append(column.name)
                table_now.append_column(column)
        _, __, expression = self._get_expression(table_now, logic_operator["condition"])
        ret = {
            "id": op_id,
            "operatorName": "Join",
            "inputs": logic_operator["inputs"],
            "condition": expression,
            "outputs": [table_now.to_dict()],
            "joinType": logic_operator["joinType"]
        }
        context.outputs.append([table_now])
        return ret


class Filter(MPCOperatorTransporterBase):
    """
    Currently, we only support single-party filter operations.
    More filter operations will be supported in the future.
    """

    def transport(self, context: "MPCTransporterContext", logic_operator):
        op_id = logic_operator["id"]
        last_schema = context.outputs[int(op_id) - 1][0]
        ret = {"id": op_id, "operatorName": "Filter"}
        condition_party, condition_type, expression = self._get_expression(last_schema, logic_operator["condition"])
        table_now = copy.deepcopy(last_schema)
        table_now.name = f"{context.config.task_id}_{op_id}"
        if condition_party == Party.SHARE:
            raise MPCSQLTransporterException("now only support condition party is local.")
        if condition_type != ColumnType.BOOLEAN:
            raise MPCSQLTransporterException("now only support condition type is boolean.")
        ret["condition"] = expression
        ret["outputs"] = [table_now.to_dict()]
        context.outputs.append([table_now])
        return ret


class Aggregate(MPCOperatorTransporterBase):
    """
    The Aggregate operation primarily involves the inference of types and parties.
    Currently, we only support scenarios involving GROUP BY. If the columns used for
    grouping and the columns involved in the computation belong to the same party,
    then the result is also attributed to that party. Otherwise, the result is marked
    as "shared," which necessitates MPC computation.
    """

    def transport(self, context: "MPCTransporterContext", logic_operator):
        op_id = logic_operator["id"]
        last_schema = context.outputs[int(op_id) - 1][0]
        table_now = Schema(f"{context.config.task_id}_{op_id}", last_schema.party)
        if len(logic_operator["group"]) != 1:
            raise MPCSQLTransporterException("now only support group by one column")
        group_column = last_schema.columns[logic_operator["group"][0]]
        aggregates_tmp = []
        table_now.append_column(group_column)
        group_party = group_column.party

        def get_type_now(type):
            if type == "BIGINT":
                return ColumnType.INT
            if type == "DOUBLE":
                return ColumnType.DOUBLE
            raise MPCSQLTransporterException(f"now only support type is int or double, but get {type}")

        for item in logic_operator["aggs"]:
            if item["agg"]["name"] == "COUNT":
                if group_party == Party.SHARE:
                    party_now = Party.SHARE
                else:
                    party_now = group_party
            else:
                column_last = last_schema.columns[item["operands"][0]]
                party_last = column_last.party
                if party_last != group_party:
                    party_now = Party.SHARE
                else:
                    party_now = party_last
            column_now = Column(item["name"].replace("$", ""), get_type_now(item["type"]["type"]), party_now)
            table_now.append_column(column_now)
            aggregate_tmp = {
                "name": item["name"],
                "aggregate": {
                    "name": item["agg"]["name"]
                },
                "distinct": item["distinct"]
            }
            if item["agg"]["name"] != "COUNT":
                aggregate_tmp["operands"] = [column_last.to_dict()]
            else:
                aggregate_tmp["operands"] = []

            aggregates_tmp.append(aggregate_tmp)
        ret = {
            "id": op_id,
            "operatorName": "Aggregate",
            "group": [group_column.to_dict()],
            "aggregates": aggregates_tmp,
            "outputs": [table_now.to_dict()]
        }
        context.outputs.append([table_now])
        return ret


class RevealTo(MPCOperatorTransporterBase):
    """
    In accordance with the characteristics of MPC, we have introduced a RevealTo
    physical execution plan to describe the ownership of the final results.
    This plan specifies which party or parties will have access to the outcome of the computations.
    """

    def transport(self, context: "MPCTransporterContext", logic_operator=None):
        op_id = len(context.outputs)
        last_schema = context.outputs[-1][0]
        ret = {"id": op_id, "operatorName": "RevealTo", "reveal_to": context.config.reveal_to.value}
        table_now = copy.deepcopy(last_schema)
        table_now.party = Party(ret["reveal_to"])
        for column in table_now.columns:
            column.party = Party(ret["reveal_to"])
        ret["outputs"] = [table_now.to_dict()]
        context.outputs.append([table_now])
        return ret
