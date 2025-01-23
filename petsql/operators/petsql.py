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

import petace.securenumpy as snp
from petace.network import NetParams, NetScheme, NetFactory
from petace.backend.duet import DuetVM
from petace.engine import PETAceEngine
from petace.setops.psi import PSI
from petace.setops import PSIScheme

from petsql.common import Config
from petsql.data import Party
from petsql.engine.cipher_engine import CipherEngine
from petsql.engine.plain_engine.data_handlers import DataHandler
from petsql.engine.plain_engine.sql_engine import SqlEngineFactory
from petsql.engine.plain_engine import PlainEngine
from petsql.compiler import MPCTransporter, MPCSQLOptimizer, SQLCompiler
from petsql.executor import PETSQLExecutor
from petsql.engine.mpc_sql_vm.vm import VM

from .operator_base import OperatorBase


class PETSQL(OperatorBase):

    def run(self, configmap: dict, config_manager: "ConfigManager" = None):
        """
        {
            # for agent
            "common": {
                "network_mode": "petnet",
                "network_scheme": "agent",
                "shared_topic": "test_network_config",
                "parties": {
                    "party_a": {"address": ["127.0.0.1:1235"]},
                    "party_b": {"address": ["127.0.0.1:1235"]}},
            }
            # for socket
            "common": {
                "network_mode": "petnet",
                "network_scheme": "socket",
                "parties": {
                    "party_a": {
                        "address": ["127.0.0.1:8090"]
                    },
                    "party_b": {
                        "address": ["127.0.0.1:8091"]
                    }
                },
                "sql": "select * from table_a",
                "config": {
                    "mode": "memory",
                    "schemas": [{
                        "name": "table_from_a",
                        "columns": [{
                            "name": "id1",
                            "type": 2,
                            "party": 0
                        }, {
                            "name": "id2",
                            "type": 2,
                            "party": 0
                        }, {
                            "name": "f1",
                            "type": 3,
                            "party": 0
                        }],
                        "party": 0
                    }, {
                        "name": "table_from_b",
                        "columns": [{
                            "name": "id1",
                            "type": 2,
                            "party": 1
                        }, {
                            "name": "id2",
                            "type": 2,
                            "party": 1
                        }, {
                            "name": "f1",
                            "type": 3,
                            "party": 1
                        }, {
                            "name": "f2",
                            "type": 3,
                            "party": 1
                        }, {
                            "name": "f3",
                            "type": 2,
                            "party": 1
                        }],
                        "party": 1
                    }],
                    "table_url": {
                        "table_from_a": "./tests/test_data/csv/table_from_a.csv",
                        "table_from_b": "./tests/test_data/csv/table_from_b.csv"
                    },
                    "engine_url": "memory:///",
                    "reveal_to": 0,
                    "task_id": "test_task_id"
                }
            },
            "party_a": {
                "inputs": {
                    "data": ["./tests/test_data/csv/table_from_a.csv"]
                },
                "outputs": {
                    "data": ["./tests/test_data/csv/test_ret.csv"]
                }
            },
            "party_b": {
                "inputs": {
                    "data": ["./tests/test_data/csv/table_from_b.csv"]
                },
                "outputs": {
                    "data": []
                }
            }
        }
        """
        common = configmap["common"]
        net_mode = common["network_mode"]
        net_scheme = common["network_scheme"]
        if net_mode != "petnet":
            raise TypeError("Only support petnet with socket scheme")

        net_cluster_def = self._trans_parties(common["parties"])

        if net_scheme == "socket":

            def get_ip_port(party_id, cluster_def):
                ip_port = cluster_def[party_id]["address"][0].split(":")
                ip = ip_port[0]
                port = int(ip_port[1])
                return ip, port

            _, my_port = get_ip_port(self.party_id, net_cluster_def)
            oppo_addr, oppo_port = get_ip_port(1 - self.party_id, net_cluster_def)

            net_params = NetParams()
            net_params.remote_addr = oppo_addr
            net_params.remote_port = oppo_port
            net_params.local_port = my_port

            net = NetFactory.get_instance().build(NetScheme.SOCKET, net_params)
        elif net_scheme == "agent":

            def get_remote_party(partys):
                for party in partys:
                    if party == self.party:
                        continue
                    return party

            net_params = NetParams()
            net_params.shared_topic = common["shared_topic"]
            net_params.local_agent = common["parties"][self.party]["address"][0]
            net_params.remote_party = get_remote_party(common["parties"])
            net = NetFactory.get_instance().build(NetScheme.AGENT, net_params)
        duet = DuetVM(net, self.party_id)
        engine = PETAceEngine(duet)
        snp.set_engine(engine)
        psi = PSI(net, self.party_id, PSIScheme.ECDH_PSI)
        config = Config()
        config.from_dict(configmap["common"]["config"])
        sql = configmap["common"]["sql"]
        cipher_engine = CipherEngine(duet, psi, config.mode)
        data_handler = DataHandler()
        sql_engine = SqlEngineFactory.create_engine(config.engine_url)
        plain_engine = PlainEngine(data_handler, sql_engine, config.mode)
        sql_vm = VM(Party(self.party_id), cipher_engine, plain_engine, mode=config.mode)
        executor = PETSQLExecutor(Party(self.party_id), SQLCompiler("./"), MPCTransporter(), MPCSQLOptimizer(), sql_vm)
        ret = executor.exec_sql(sql, config)
        if self.party_id == config.reveal_to.value:
            data_handler.write(configmap[self.party]["outputs"]["data"][0],
                               ret.plain_data.data,
                               ret.schema.name,
                               columns=ret.schema.columns)
        return True
