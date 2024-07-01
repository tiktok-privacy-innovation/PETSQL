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

from typing import List, Tuple, Dict
from petsql.data.schema import Schema, ColumnType
from petsql.common.config import Mode
from petsql.engine.plain_engine.exception import PlainEngineParserException
from petsql.compiler.mpc_transporter import ArithOperator, BooleanOperator


class Parser:
    """
    Reconstruct SQL based on the physical execution plan and schema
    information for use by a unified execution engine.
    """

    def _build_expression_recursion(self, expression: Dict) -> str:
        """
        Recursively build the expression.

        Parameters
        ----------
        expression : Dict
            Expression dict.

        Returns
        -------
        str
            The expression string.
        """
        if "literal" in expression:
            value = expression["literal"]
            if isinstance(value, str):
                value = f"'{value}'"
            return str(value)
        if "input" in expression:
            return f"{expression['input']['name']}"
        if "operator" in expression:
            op_name = expression["operator"]
            operands = expression["operands"]
            if len(operands) != 2:
                raise PlainEngineParserException("Unsupported or Invalid Expression")
            if not ArithOperator.is_airth_op(op_name) and not BooleanOperator.is_boolean_op(op_name):
                raise PlainEngineParserException(f"Unsupport operator: {op_name}")
            left_operand = self._build_expression_recursion(operands[0])
            right_operand = self._build_expression_recursion(operands[1])
            return f"({left_operand} {op_name} {right_operand})"
        raise PlainEngineParserException("Unsupported expression")

    def _build_join_recursion(self, expression: Dict, remapping: Dict) -> str:
        """
        Recursively build the join expression.

        Parameters
        ----------
        expression : Dict
            Expression dict.
        remapping : Dict
            Remapping dict. When columns have the same name in a join, rewriting will be performed.

        Returns
        -------
        str
            The join expression string.
        """
        if "literal" in expression:
            value = expression["literal"]
            if isinstance(value, str):
                value = f"'{value}'"
            return str(value)
        if "input" in expression:
            return f"{remapping.get(expression['input']['name'])}"
        if "operator" in expression:
            op_name = expression["operator"]
            operands = expression["operands"]
            if len(operands) != 2:
                raise PlainEngineParserException("Unsupported or Invalid Expression")
            if not ArithOperator.is_airth_op(op_name) and not BooleanOperator.is_boolean_op(op_name):
                raise PlainEngineParserException(f"Unsupport operator: {op_name}")
            left_operand = self._build_join_recursion(operands[0], remapping)
            right_operand = self._build_join_recursion(operands[1], remapping)
            return f"({left_operand} {op_name} {right_operand})"
        raise PlainEngineParserException("Unsupported expression")

    def parse_project_plan_to_sql(self, plan: Dict, last_schema: "Schema", mode: "Mode") -> Tuple[str, "Schema"]:
        """
        Convert a project plan into SQL code.

        Parameters
        ----------
        plan : Dict
            The project plan.
        last_schema : Schema
            The last schema.
        mode : Mode
            The mode.

        Returns
        -------
        Tuple[str, Schema]
            The SQL code and the output schema.
        """
        select_clause = []
        output_table_schema = Schema().from_dict(plan["outputs"][0])
        output_columns_names = [column.name for column in output_table_schema.columns]
        for column_name, expression in zip(output_columns_names, plan["expressions"]):
            if "input" in expression:
                select_clause.append(f"{column_name} AS {column_name}")
            elif "literal" in expression:
                value = expression["literal"]
                if isinstance(value, str):
                    value = f"'{value}'"
                select_clause.append(str(value))
            elif "operator" in expression:
                project_sub_expression = self._build_expression_recursion(expression)
                select_clause.append(f"{project_sub_expression} AS {column_name}")
        select_statement = ", ".join(select_clause)
        table_from_name = last_schema.name
        output_table_name = output_table_schema.name
        project_sql = f"SELECT {select_statement} FROM {table_from_name}"
        if mode == Mode.MEMORY:
            project_sql += f" AS {output_table_name};"
        else:
            project_sql = f"INSERT INTO {output_table_name} " + project_sql + ";"
        return project_sql, output_table_schema

    def parse_filter_plan_to_sql(self, plan: Dict, last_schema: "Schema", mode: "Mode") -> Tuple[str, "Schema"]:
        """
        Convert a filter plan into SQL code.

        Parameters
        ----------
        plan : Dict
            The filter plan.
        last_schema : Schema
            The last schema.
        mode : Mode
            The mode.

        Returns
        -------
        Tuple[str, Schema]
            The SQL code and the output schema.
        """
        if plan["operatorName"] != "Filter":
            raise PlainEngineParserException("Only Filter operation can be processed.")
        where_clause = self._build_expression_recursion(plan["condition"])
        table_from_name = last_schema.name
        output_table_schema = Schema().from_dict(plan["outputs"][0])
        output_table_name = output_table_schema.name
        if mode == Mode.MEMORY:
            where_sql = f"SELECT * FROM {table_from_name} AS {output_table_name} WHERE {where_clause};"
        else:
            where_sql = f"INSERT INTO {output_table_name} SELECT * FROM {table_from_name} WHERE {where_clause};"
        return where_sql, output_table_schema

    def parse_join_plan_to_sql(self, plan: Dict, last_schemas: List["Schema"], mode: "Mode") -> Tuple[str, "Schema"]:
        """
        Convert a join plan into SQL code.

        Parameters
        ----------
        plan : Dict
            The join plan.
        last_schemas : List[Schema]
            The last schemas.
        mode : Mode
            The mode.

        Returns
        -------
        Tuple[str, Schema]
            The SQL code and the output schema.
        """
        if plan["operatorName"] != "Join":
            raise PlainEngineParserException("Only Join operation can be processed.")
        table_columns = [str(last_schemas[0].name + "." + column.name) for column in last_schemas[0].columns]
        table_columns_1 = [str(last_schemas[1].name + "." + column.name) for column in last_schemas[1].columns]
        table_columns.extend(table_columns_1)
        output_table_schema = Schema().from_dict(plan["outputs"][0])
        output_table_columns_name = [column.name for column in output_table_schema.columns]
        remapping = dict(zip(output_table_columns_name, table_columns))

        join_clause = self._build_join_recursion(plan["condition"], remapping)
        output_table_name = output_table_schema.name
        if mode == Mode.MEMORY:
            join_sql = f"SELECT * FROM (SELECT * FROM {last_schemas[0].name} JOIN {last_schemas[1].name} ON {join_clause}) AS {output_table_name};"
        else:
            join_sql = f"INSERT INTO {output_table_name} SELECT * FROM {last_schemas[0].name} JOIN {last_schemas[1].name} ON {join_clause};"
        return join_sql, output_table_schema

    def parse_aggregate_plan_to_sql(self, plan: Dict, last_schema: "Schema", mode: "Mode") -> Tuple[str, "Schema"]:
        """
        Convert an aggregate plan into SQL code.

        Parameters
        ----------
        plan : Dict
            The aggregate plan.
        last_schema : Schema
            The last schema.
        mode : Mode
            The mode.

        Returns
        -------
        Tuple[str, Schema]
            The SQL code and the output schema.
        """
        if plan["operatorName"] != "Aggregate":
            raise PlainEngineParserException("Only Aggregate operation can be processed.")
        aggregate_expressions = []
        for agg in plan["aggregates"]:
            agg_name = agg["aggregate"]["name"]
            if agg_name != "COUNT":
                operand = agg["operands"][0]["name"]
            else:
                operand = "*"
            agg_as_name = agg["name"]
            aggregate_expressions.append(f"{agg_name}({operand}) AS {agg_as_name}")
        group_by_caluse = ", ".join([grp["name"] for grp in plan["group"]])
        aggregate_caluse = ", ".join([grp["name"] for grp in plan["group"]] + aggregate_expressions)

        table_from_name = last_schema.name
        output_table_schema = Schema().from_dict(plan["outputs"][0])
        output_table_name = output_table_schema.name
        if mode == Mode.MEMORY:
            aggregate_sql = f"SELECT * FROM (SELECT {aggregate_caluse} FROM {table_from_name} GROUP BY {group_by_caluse}) AS {output_table_name};"
        else:
            aggregate_sql = f"INSERT INTO {output_table_name} SELECT {aggregate_caluse} FROM {table_from_name} GROUP BY {group_by_caluse};"
        return aggregate_sql, output_table_schema

    def create_output_table_sql_from_plan(self, plan: Dict) -> str:
        """
        Create the output table sql according to the plan.

        Parameters
        ----------
        plan : Dict
            The physical plan.

        Returns
        -------
        str
            The create table sql.
        """

        output_table_schema = Schema().from_dict(plan["outputs"][0])
        output_table_name = output_table_schema.name
        column_clause = []
        for column in output_table_schema.columns:
            column_clause.append(f"{column.name} {ColumnType.to_sql_type(column.type)}")
        column_clause = ", ".join(column_clause)
        create_table_sql = f"CREATE TABLE IF NOT EXISTS {output_table_name}({column_clause});"
        return create_table_sql
