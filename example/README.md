# PETSQL Examples

We illustrate three examples here to show you how to use PETSQL.

## How to use PETSQL for SQL computations

We assume that PETSQL has been correctly installed.

We have provided all the processes in the `example.py`.

To run the examples, open two terminal sessions and run this command in the first terminal:

```shell
python3 ./example/example.py -p 0
```

```shell
python3 ./example/example.py -p 1
```

Next, we will explain each step.

First, we generated data for both parties separately.


```python
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
```

Next, we registered the data schema and configured some runtime information.

```python
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
```

Then, we began initializing all components of the PETSQL Executor and registering them with the Executor.


```python
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
```

Now, we can proceed with SQL computations.

```python
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
```

You can print the final results using the following code:

```python
print(ret.plain_data.data)
```
