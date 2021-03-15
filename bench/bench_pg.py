import pandas
import psycopg2
from numpy import arange, sin
from tanker import View, connect, create_tables

from lakota import Repo, Schema
from lakota.utils import timeit


def write_lk(df):
    schema = Schema(timestamp="timestamp*", value="float")
    repo = Repo("test-db")
    collection = repo.create_collection(schema, "test")
    series = collection / "test"
    series.write(df)


def write_pg(df):
    tables = [
        {
            "table": "test",
            "columns": {"timestamp": "timestamp", "value": "float"},
            "key": ["timestamp"],
            "use-index": "brin",
        }
    ]
    cfg = {
        "db_uri": "postgresql:///test",
        "schema": tables,
    }
    with connect(cfg):
        create_tables()
    with connect(cfg):
        View("test").write(df)


def read_lk():
    repo = Repo("test-db")
    collection = repo / "test"
    series = collection / "test"
    frm = series.frame()
    print(len(frm))


def read_pg():
    conn = psycopg2.connect("postgresql:///test")
    cursor = conn.cursor()
    cursor.execute("select * from test")
    data = list(cursor)
    print(len(data))


ts = pandas.date_range("1970-01-01", "2020-01-01", freq="5min")
value = sin(arange(len(ts)))
df = pandas.DataFrame(
    {
        "timestamp": ts,
        "value": value,
    }
)


with timeit("write pg"):
    write_pg(df)
with timeit("write lk"):
    write_lk(df)
with timeit("read pg"):
    read_pg()
with timeit("read lk"):
    read_lk()

# $ p bench/bench_pg.py
# write pg 83.74s
# write lk 375.67ms
# 5259457
# read pg 10.03s
# 5259457
# read lk 193.10ms

# $ psql test -c "SELECT pg_size_pretty(pg_database_size('test'))"
#  pg_size_pretty
# ----------------
#  382 MB
# (1 row)
#  du -hs .lakota/
# 2.0M    .lakota/
