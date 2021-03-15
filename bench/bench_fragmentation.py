# This code show the performance impact of data fragmentation (both in
# timing and on-disk size).

# **Script output**
# chunk size 500: 23.86s
# Disk use 972M
# chunk size 5000: 1.06s
# Disk use 19M
# chunk size 50000: 148.83ms
# Disk use 3.5M
# chunk size 500000: 57.62ms
# Disk use 3.4M


import subprocess

from numpy import arange, random
from pandas import DataFrame

from lakota import Repo, Schema
from lakota.utils import timeit

SIZE = 1_000_000

CHUNK_SIZES = (500, 5_000, 50_000, 500_000)


def create_df(start, stop):
    ts = arange(start, stop)
    value = arange(start, stop)
    random.shuffle(value)

    return DataFrame({"timestamp": ts, "value": value})


def call(cmd):
    proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
    stdout, _ = proc.communicate()
    return stdout


for chunk_size in CHUNK_SIZES:
    df = create_df(0, SIZE)
    with timeit(f"chunk size {chunk_size}:"):
        schema = Schema(timestamp="timestamp*", value="float")
        repo = Repo("test-db")
        collection = repo.create_collection(schema, "test")
        series = collection / "test"
        for i in range(0, SIZE, chunk_size):
            series.write(df[i : i + chunk_size])
    res = call("du -hs test-db")
    print("Disk use", res.split()[0].decode())
    call("rm -r test-db")
