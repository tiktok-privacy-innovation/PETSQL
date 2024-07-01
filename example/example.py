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
import random
import pandas as pd
from pandasql import sqldf

from petace.network import NetParams, NetScheme, NetFactory
from petace.duet import VM as DuetVM
from petace.setops import PSI, PSIScheme
import petace.securenumpy as snp

from petsql.data import Column, ColumnType, Party
from petsql.data import Schema
from petsql.common import Config, Mode
from petsql.engine.cipher_engine import CipherEngine
from petsql.engine.plain_engine.data_handlers import DataHandler
from petsql.engine.plain_engine.sql_engine import SqlEngineFactory
from petsql.engine.plain_engine import PlainEngine
from petsql.compiler import MPCTransporter, MPCSQLOptimizer, SQLCompiler
from petsql.executor import PETSQLExecutor
from petsql.engine.mpc_sql_vm.vm import VM


def gen_example_data(data_path, party):
    if party == 0:
        print("gen data for party 0")
        columns = ["id1", "id2", "f1"]
        ret = []
        for i in range(10):
            ret.append([i, i, random.uniform(-100, 100)])
        df = pd.DataFrame(ret, columns=columns)
        os.makedirs(f"{data_path}/csv", exist_ok=True)
        print(df)
        df.to_csv(f"{data_path}/csv/data_a.csv", index=False)
        print(f"gen data for party 0, save to {data_path}/csv/data_a.csv")
    if party == 1:
        print("gen data for party 1")
        columns = ["id1", "id2", "f1", "f2", "f3"]
        ret = []
        for i in range(20):
            ret.append([i, i, random.uniform(-100, 100), random.uniform(-100, 100), random.randint(0, 5)])
        df = pd.DataFrame(ret, columns=columns)
        os.makedirs(f"{data_path}/csv", exist_ok=True)
        df.to_csv(f"{data_path}/csv/data_b.csv", index=False)
        print(f"gen data for party 1, save to {data_path}/csv/data_b.csv")
    return df


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='PETSQL demo.')
    parser.add_argument("--data_path", type=str, help="data path", default=os.path.dirname(os.path.abspath(__file__)))
    parser.add_argument("-p", "--party", type=int, help="which party")
    parser.add_argument("--port0", type=int, help="port of party 0, defalut 8089", default=8089)
    parser.add_argument("--port1", type=int, help="port of party 1, defalut 8090", default=8090)
    parser.add_argument("--host", type=str, help="host of this party", default="127.0.0.1")

    args = parser.parse_args()
    # Generate example data
    my_df = gen_example_data(args.data_path, args.party)

    # init schema and config

    col00 = Column("id1", ColumnType.INT)
    col01 = Column("id2", ColumnType.INT)
    col02 = Column("f1", ColumnType.DOUBLE)
    schema_a = Schema("table_a", Party.ZERO)
    schema_a.append_column(col00)
    schema_a.append_column(col01)
    schema_a.append_column(col02)

    col10 = Column("id1", ColumnType.INT)
    col11 = Column("id2", ColumnType.INT)
    col12 = Column("f1", ColumnType.DOUBLE)
    col13 = Column("f2", ColumnType.DOUBLE)
    col14 = Column("f3", ColumnType.INT)
    schema_b = Schema("table_b", Party.ONE)
    schema_b.append_column(col10)
    schema_b.append_column(col11)
    schema_b.append_column(col12)
    schema_b.append_column(col13)
    schema_b.append_column(col14)

    config = Config()
    config.schemas = [schema_a, schema_b]
    config.table_url = {"table_a": f"{args.data_path}/csv/data_a.csv", "table_b": f"{args.data_path}/csv/data_b.csv"}
    config.engine_url = "memory:///"
    config.reveal_to = Party.ZERO
    config.mode = Mode.MEMORY
    config.task_id = "test_task_id"

    # init network
    party = args.party
    port0 = args.port0
    port1 = args.port1
    host = args.host

    net_params = NetParams()
    if party == 0:
        net_params.remote_addr = host
        net_params.remote_port = port1
        net_params.local_port = port0
    else:
        net_params.remote_addr = host
        net_params.remote_port = port0
        net_params.local_port = port1

    # init mpc engine
    net = NetFactory.get_instance().build(NetScheme.SOCKET, net_params)

    # init petsql
    duet = DuetVM(net, party)
    snp.set_vm(duet)

    psi = PSI(net, party, PSIScheme.ECDH_PSI)

    cipher_engine = CipherEngine(duet, psi, Mode.MEMORY)
    data_handler = DataHandler()
    sql_engine = SqlEngineFactory.create_engine(config.engine_url)
    plain_engine = PlainEngine(data_handler, sql_engine, Mode.MEMORY)
    sql_vm = VM(Party(party), cipher_engine, plain_engine)
    executor = PETSQLExecutor(Party(party), SQLCompiler(), MPCTransporter(), MPCSQLOptimizer(), sql_vm)

    # sql
    sql = """
    SELECT
        b.f3 as f3,
        sum(b.f1) as sum_f,
        sum(b.f1 + b.f2) as sum_f2,
        max(b.f1 * b.f1 + a.f1 - a.f1 / b.f1) AS max_f,
        min(b.f1 * a.f1 + 1) as min_f
    FROM (select id1, id2, f1 from table_a where f1 < 90) AS a
    JOIN (select id1, id2, f1 + f2 + 2.01 as f1, f1 * f2 + 1 as f2, f3 from table_b) AS b ON a.id1 = b.id1
    GROUP BY b.f3
    """

    ret = executor.exec_sql(sql, config)

    print("My data: ")
    print(my_df)

    if party == 0:
        table_a = DataHandler().read(config.table_url["table_a"], "table_a")
        table_b = DataHandler().read(config.table_url["table_b"], "table_b")
        aim_ret = sqldf(sql, {"table_a": table_a, "table_b": table_b})
        aim_ret.columns = aim_ret.columns.str.upper()
        print("PETSQL result:")
        print(ret.plain_data.data)

        print("pandasql result:")
        print(aim_ret)

        print("abs < 0.1 ? :")
        print(((ret.plain_data.data - aim_ret).abs() < 0.1).all().all())
