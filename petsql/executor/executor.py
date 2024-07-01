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

from petsql.data import Party
from petsql.engine.mpc_sql_vm.vm import VM
from petsql.engine.mpc_sql_vm.register import RegisterItem
from petsql.compiler import SQLCompiler, MPCSQLOptimizer, MPCTransporter
from petsql.common import Config


class PETSQLExecutor:
    """
    PETSQLExecutor is a class that executes SQL statements.

    Parameters
    ----------
    party : Party
        The party of PETSQLExecutor.
    compiler : SQLCompiler
        The compiler of the PETSQL.
    mpc_transporter : MPCTransporter
        The mpc transporter of the PETSQL.
    optimizer : MPCSQLOptimizer
        The optimizer of the PETSQL.
    vm : VM
        The vm of the PETSQL.

    Attributes
    ----------
    party : Party
        The party of the PETSQL.
    compiler : SQLCompiler
        The compiler of the PETSQL.
    mpc_transporter : MPCTransporter
        The mpc transporter of the PETSQL.
    optimizer : MPCSQLOptimizer
        The optimizer of the PETSQL.
    vm : VM
        The vm of the PETSQL.
    """
    party = None
    compiler = None
    mpc_transporter = None
    optimizer = None
    vm = None

    def __init__(self, party: Party, compiler: SQLCompiler, mpc_transporter: MPCTransporter, optimizer: MPCSQLOptimizer,
                 vm: VM) -> None:
        self.party = party
        self.compiler = compiler
        self.mpc_transporter = mpc_transporter
        self.optimizer = optimizer
        self.vm = vm

    def exec_sql(self, sql: str, config: Config) -> RegisterItem:
        """
        Execute SQL statement.

        Parameters
        ----------
        sql: str
            The SQL statement.
        config: Config
            The config of the SQL statement.

        Returns
        -------
        RegisterItem
            The result of the SQL statement.

        """
        self.vm.clean()
        logic_plan = self.compiler.compile(sql, config)
        physical_plan = self.mpc_transporter.transport(config, logic_plan)
        opt_physical_plan = self.optimizer.optimize(config, physical_plan)
        for item in opt_physical_plan:
            self.vm.exec_plan(item)
        return self.vm.reg.get_data(-1)
