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
import json
import pandas as pd
from pandasql import sqldf
import petace.securenumpy as snp
from petace.engine import PETAceEngine
from petace.backend.bigdata import BigDataVM
from petace.setops.bigdata_setops import BigdataSetOps
from crypto_data_engine import Context
from petsql.engine.cipher_engine import CipherEngine
from petsql.engine.plain_engine.data_handlers import SparkDataHandler, DataHandler
from petsql.engine.plain_engine.sql_engine import SqlEngineFactory
from petsql.data import Column, ColumnType, Party
from petsql.data import Schema
from petsql.common import Config, Mode
from petsql.engine.plain_engine import PlainEngine
from petsql.engine.mpc_sql_vm.vm import VM
from petsql.compiler import MPCTransporter, MPCSQLOptimizer, SQLCompiler
from petsql.executor import PETSQLExecutor


def gen_example_data(data_path, party):
    if party == 0:
        print("gen data for party 0")
        columns = ["id1", "id2", "f1"]
        ret = []
        for i in range(10):
            ret.append([i, i, random.uniform(-100, 100)])
        df = pd.DataFrame(ret, columns=columns)
        os.makedirs(f"{data_path}/csv", exist_ok=True)
        df.to_csv(f"{data_path}/csv/table_from_a.csv", index=False)
        print(f"gen data for party 0, save to {data_path}/csv/table_from_a.csv")
    if party == 1:
        print("gen data for party 1")
        columns = ["id1", "id2", "f1", "f2", "f3"]
        ret = []
        for i in range(20):
            ret.append([i, i, random.uniform(-100, 100), random.uniform(-100, 100), random.randint(0, 5)])
        df = pd.DataFrame(ret, columns=columns)
        os.makedirs(f"{data_path}/csv", exist_ok=True)
        df.to_csv(f"{data_path}/csv/table_from_b.csv", index=False)
        print(f"gen data for party 1, save to {data_path}/csv/table_from_b.csv")
    return df


if __name__ == "__main__":
    import argparse
    dir = os.path.dirname(os.path.abspath(__file__))
    parser = argparse.ArgumentParser(description='PETSQL bigdata demo.')
    parser.add_argument("-p", "--party", type=int, help="which party")
    parser.add_argument("--data_path", type=str, help="data path", default=dir)
    parser.add_argument("--port0", type=int, help="port of party 0, defalut 18000", default=18000)
    parser.add_argument("--port1", type=int, help="port of party 1, defalut 19000", default=19000)
    parser.add_argument("--host", type=str, help="host of this party", default="127.0.0.1")
    parser.add_argument('-et', "--engine_type", type=str, help="engine type", default="local")
    parser.add_argument('--core', type=int, help="test_num", default=2)
    parser.add_argument('--part', type=int, help="test_num", default=2)

    args = parser.parse_args()
    data_path = args.data_path
    party = args.party
    port0 = args.port0
    port1 = args.port1
    host = args.host
    engine_type = args.engine_type
    core = args.core
    part = args.part

    my_df = gen_example_data(data_path, args.party)

    duet_parties = {
        '0': {
            "remote_addr": host,
            "remote_port_base": port1,
            "local_port_base": port0
        },
        '1': {
            "remote_addr": host,
            "remote_port_base": port0,
            "local_port_base": port1
        }
    }
    duet_engine_ctx_params = {
        "work_directory": dir + "/bigdata/test_data/duet",
        'engine_type': engine_type,
        'max_workers': core
    }

    bigdata = BigDataVM(party, duet_engine_ctx_params, "socket", duet_parties, part)
    engine = PETAceEngine(bigdata)
    snp.set_engine(engine)

    psi_engine_ctx_parmas = {
        "work_directory": dir + "/bigdata/test_data/psi",
        'engine_type': engine_type,
        'max_workers': core
    }

    psi_ctx = Context(**psi_engine_ctx_parmas)

    psi_parties = {
        '0': {
            "remote_addr": host,
            "remote_port_base": port1 + 10000,
            "local_port_base": port0 + 10000
        },
        '1': {
            "remote_addr": host,
            "remote_port_base": port0 + 10000,
            "local_port_base": port1 + 10000
        }
    }

    psi = BigdataSetOps(party, psi_ctx, "socket", psi_parties)
    cipher_engine = CipherEngine(engine, psi, Mode.SPARK)
    data_handler = SparkDataHandler()

    col00 = Column("id1", ColumnType.INT)
    col01 = Column("id2", ColumnType.INT)
    col02 = Column("f1", ColumnType.DOUBLE)
    schema0 = Schema("table_from_a", Party.ZERO)
    schema0.append_column(col00)
    schema0.append_column(col01)
    schema0.append_column(col02)

    col10 = Column("id1", ColumnType.INT)
    col11 = Column("id2", ColumnType.INT)
    col12 = Column("f1", ColumnType.DOUBLE)
    col13 = Column("f2", ColumnType.DOUBLE)
    col14 = Column("f3", ColumnType.INT)
    schema1 = Schema("table_from_b", Party.ONE)
    schema1.append_column(col10)
    schema1.append_column(col11)
    schema1.append_column(col12)
    schema1.append_column(col13)
    schema1.append_column(col14)

    spark_config = {
        "app_name": "Local Spark",
        "master": f"local[{core}]",
        "warehouse_dir": dir + f"/bigdata/spark/{party}",
        "metastore_db_dir": dir + f"/bigdata/spark/{party}/metastore_db"
    }

    config = Config()
    config.schemas = [schema0, schema1]
    dir = os.path.dirname(os.path.abspath(__file__))
    config.table_url = {"table_from_a": dir + "/csv/table_from_a.csv", "table_from_b": dir + "/csv/table_from_b.csv"}
    config.engine_url = "spark:///" + json.dumps(spark_config)
    config.reveal_to = Party.ZERO
    config.mode = Mode.SPARK
    config.task_id = "test_task_id"
    sql_engine = SqlEngineFactory.create_engine(config.engine_url)
    plain_engine = PlainEngine(data_handler, sql_engine, Mode.SPARK)
    sql_vm = VM(Party(party), cipher_engine, plain_engine, mode=Mode.SPARK)

    executor = PETSQLExecutor(Party(party), SQLCompiler("./"), MPCTransporter(), MPCSQLOptimizer(), sql_vm)
    sql = """
    SELECT
        b.f3 as f3,
        sum(b.f1) as sum_f,
        sum(b.f1 + b.f2) as sum_f2,
        max(b.f1 * b.f1 + a.f1 - a.f1 / b.f1) AS max_f,
        min(b.f1 * a.f1 + 1) as min_f
    FROM (select id1, id2, f1 from table_from_a where f1 < 90) AS a
    JOIN (select id1, id2, f1 + f2 + 2.01 as f1, f1 * f2 + 1 as f2, f3 from table_from_b) AS b ON a.id1 = b.id1
    GROUP BY b.f3
    """

    ret = executor.exec_sql(sql, config)
    print("My data: ")
    print(my_df)

    if party == 0:
        ret_engine = SqlEngineFactory.create_engine(config.engine_url)
        print("Real ret:")
        ret = ret_engine.execute(f"select * from {ret.plain_data.schema.name}", True)
    if party == 0:
        table_from_a = DataHandler().read(config.table_url["table_from_a"], "table_from_a")
        table_from_b = DataHandler().read(config.table_url["table_from_b"], "table_from_b")
        aim_ret = sqldf(sql, {"table_from_a": table_from_a, "table_from_b": table_from_b})
        aim_ret.columns = aim_ret.columns.str.upper()
        print(aim_ret)
