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

import os
import json
from typing import Dict
from pandasql import sqldf

from petace.network import NetParams, NetScheme, NetFactory
from petace.backend.duet import DuetVM
from petace.setops.psi import PSI
from petace.setops import PSIScheme

import petace.securenumpy as snp
from petace.engine import PETAceEngine
from petace.backend.bigdata import BigDataVM
from petace.setops.bigdata_setops import BigdataSetOps

from crypto_data_engine import Context

from petsql.engine.cipher_engine import CipherEngine
from petsql.common import Mode
from petsql.data import Party
from petsql.engine.plain_engine.data_handlers import DataHandler, SparkDataHandler
from petsql.engine.plain_engine.sql_engine import SqlEngineFactory
from petsql.engine.plain_engine import PlainEngine
from petsql.compiler import MPCTransporter, MPCSQLOptimizer, SQLCompiler
from petsql.executor import PETSQLExecutor
from petsql.engine.mpc_sql_vm.vm import VM

from .process import Process
from .config import TestConfig


class TestDataUtils:

    @staticmethod
    def write_json_to_file(dict_to_write: Dict, name: str, dir: str = None) -> None:
        dir = os.path.dirname(os.path.abspath(__file__)) + "/test_data/json/"
        with open(dir + name, "w") as f:
            f.write(json.dumps(dict_to_write))

    @staticmethod
    def load_json_from_file(name: str, dir: str = None) -> Dict:
        dir = os.path.dirname(os.path.abspath(__file__)) + "/test_data/json/"
        with open(dir + name, "r") as f:
            json_str = f.read()
        return json.loads(json_str)

    @staticmethod
    def get_sql_result(config: "Config", sql: str):
        table_from_a = DataHandler().read(config.table_url["table_from_a"], "table_from_a")
        table_from_b = DataHandler().read(config.table_url["table_from_b"], "table_from_b")
        aim_ret = sqldf(sql, {"table_from_a": table_from_a, "table_from_b": table_from_b})
        aim_ret.columns = aim_ret.columns.str.upper()
        return aim_ret


class PETSQLTestException(Exception):

    def __init__(self, party, message) -> None:
        self.party = party
        self.message = message

    def __str__(self):
        return f"party {self.party} test failed: {self.message}"


class CommonTestBase:

    def run_test_all(self):
        p = Process(target=self.run_process, args=())
        p.start()
        p.join()
        if p.exception:
            error, _ = p.exception
            p.terminate()
            raise PETSQLTestException(0, error)

    def run_process(self):
        for method in dir(self):
            if method.startswith("test_"):
                getattr(self, method)()


def run_test(test_func):
    processes = []

    for i in range(2):
        p = Process(target=test_func, args=(i,))
        p.start()
        processes.append(p)
    for p in processes:
        p.join()

    for n, p in enumerate(processes):
        if p.exception:
            error, _ = p.exception
            p.terminate()
            raise PETSQLTestException(n, error)


def init_network(party: int):
    net_params = NetParams()
    ip1 = "127.0.0.1"
    port1 = 17890
    ip2 = "127.0.0.1"
    port2 = 17891
    if party == 0:
        net_params.remote_addr = ip1
        net_params.remote_port = port1
        net_params.local_port = port2
    else:
        net_params.remote_addr = ip2
        net_params.remote_port = port2
        net_params.local_port = port1
    net = NetFactory.get_instance().build(NetScheme.SOCKET, net_params)
    return net


def init_duet(net: "Network", party: int):
    duet = DuetVM(net, party)
    vm = PETAceEngine(duet)
    snp.set_engine(vm)
    return vm


def init_psi(net: "Network", party: int):
    psi = PSI(net, party, PSIScheme.ECDH_PSI)
    return psi


def init_spark_duet(_net: "Network", party: int):
    dir = os.path.dirname(os.path.abspath(__file__))
    parties = {
        '0': {
            "remote_addr": "127.0.0.1",
            "remote_port_base": 12000,
            "local_port_base": 13000
        },
        '1': {
            "remote_addr": "127.0.0.1",
            "remote_port_base": 13000,
            "local_port_base": 12000
        }
    }
    bigdata = BigDataVM(party, {"work_directory": dir + f"/test_tmp_data/{party}/cipher_data/duet"}, 'socket', parties,
                        2)
    vm = PETAceEngine(bigdata)
    snp.set_engine(vm)
    return vm


def init_spark_psi(party: int):
    dir = os.path.dirname(os.path.abspath(__file__))
    engine_ctx_params = {"work_directory": dir + f"/test_tmp_data/{party}/cipher_data/psi", 'engine_type': "local"}

    ctx = Context(**engine_ctx_params)

    parties = {
        '0': {
            "remote_addr": "127.0.0.1",
            "remote_port_base": 19000,
            "local_port_base": 18000
        },
        '1': {
            "remote_addr": "127.0.0.1",
            "remote_port_base": 18000,
            "local_port_base": 19000
        }
    }
    return BigdataSetOps(party, ctx, "socket", parties)


class PETSQLTestBase:

    def run_test_all(self):
        run_test(self.run_process)

    def _init_executor(self, party: int, mode: "Mode"):
        self.net = init_network(party)
        self.duet = init_duet(self.net, party)
        self.psi = init_psi(self.net, party)
        self.cipher_engine = CipherEngine(self.duet, self.psi, mode)
        self.data_handler = DataHandler()
        if mode == Mode.MEMORY:
            self.config = TestConfig.get_config()
        else:
            self.config = TestConfig.get_bigdata_config(party)
        self.sql_engine = SqlEngineFactory.create_engine(self.config.engine_url)
        self.plain_engine = PlainEngine(self.data_handler, self.sql_engine, mode)
        self.sql_vm = VM(Party(party), self.cipher_engine, self.plain_engine, mode=mode)
        self.executor = PETSQLExecutor(Party(party), SQLCompiler("./"), MPCTransporter(), MPCSQLOptimizer(),
                                       self.sql_vm)

    def _init_spark_executor(self, party: int, mode: "Mode"):
        self.net = init_network(party)
        self.duet = init_spark_duet(self.net, party)
        self.psi = init_spark_psi(party)
        self.cipher_engine = CipherEngine(self.duet, self.psi, mode)
        self.data_handler = SparkDataHandler()
        self.config = TestConfig.get_spark_config(party)
        self.sql_engine = SqlEngineFactory.create_engine(self.config.engine_url)
        self.plain_engine = PlainEngine(self.data_handler, self.sql_engine, mode)
        self.sql_vm = VM(Party(party), self.cipher_engine, self.plain_engine, mode=mode)
        self.executor = PETSQLExecutor(Party(party), SQLCompiler("./"), MPCTransporter(), MPCSQLOptimizer(),
                                       self.sql_vm)

    def run_process(self, party):
        for method in dir(self):
            if method.startswith("test_"):
                getattr(self, method)(party)
