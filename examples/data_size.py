from numpy.random import random
from pandas import DataFrame, date_range

from lakota import Repo, Schema
from lakota.utils import hextime

suffix = hextime()

SIZE = 100_000
values = random(SIZE)
timestamps = date_range("1970-01-01", freq="5min", periods=SIZE)
df = DataFrame(
    {
        "ts": timestamps,
        "value": values,
    }
)

df.to_csv(f"timeseries-{suffix}.csv")
df.to_parquet(f"timeseries-{suffix}.pqt")


repo = Repo("repo")
schema = Schema(["ts timestamp*", "value float"])
clct = repo / "my_collection"
if not clct:
    clct = repo.create_collection(schema, "my_collection")
series = clct / "my_series"
series.write(df)


# Results

# $ p data_size.py
# $ du -s repo/ timeseries*.csv timeseries*.pqt
# 704     repo/
# 4480    timeseries-1753fee3376.csv
# 3296    timeseries-1753fee3376.pqt
# (lakota) bch@wsl:~/dev/lakota/examples$ find repo/ -type f | xargs ls -lh
# -rw-r--r-- 1 bch bch  164 Oct 19 10:16 repo/00/00/0000000-0000000000000000000000000000000000000000/00000000000-0000000000000000000000000000000000000000.1753fee34ad-0420e88dba8dfab486ac0eb213c66c6ce17c7c3c
# -rw-r--r-- 1 bch bch  190 Oct 19 10:16 repo/30/37/e9bc6156d875ce2ba4a4d1db05c1b3803b48/00000000000-0000000000000000000000000000000000000000.1753fee34b3-af03a176909e1cf51646bb5232f580099d7d2fa0
# -rw-r--r-- 1 bch bch   39 Oct 19 10:16 repo/75/71/49eedbfc5eab5864122c80a7154f3adcf701
# -rw-r--r-- 1 bch bch 686K Oct 19 10:16 repo/af/14/00ce0ff1f8d4cf268cfdeaa777a3341f1d43
# -rw-r--r-- 1 bch bch  155 Oct 19 10:16 repo/b8/41/2543a21bceaefbf539bb0b51d7590b563566
# -rw-r--r-- 1 bch bch  15K Oct 19 10:16 repo/e6/0c/0ea648ae8ae4a717f29622eb75a1a4029b6e
# $ p data_size.py
# $ du -s repo/ timeseries*.csv timeseries*.pqt
# 1392    repo/
# 4480    timeseries-1753fee3376.csv
# 4416    timeseries-1753feedae9.csv
# 3296    timeseries-1753fee3376.pqt
# 3296    timeseries-1753feedae9.pqt
# (lakota) bch@wsl:~/dev/lakota/examples$ find repo/ -type f | xargs ls -lh
# -rw-r--r-- 1 bch bch  164 Oct 19 10:16 repo/00/00/0000000-0000000000000000000000000000000000000000/00000000000-0000000000000000000000000000000000000000.1753fee34ad-0420e88dba8dfab486ac0eb213c66c6ce17c7c3c
# -rw-r--r-- 1 bch bch 686K Oct 19 10:16 repo/08/67/f9f48070c67524986100d7f0b6f9feabc959
# -rw-r--r-- 1 bch bch  190 Oct 19 10:16 repo/30/37/e9bc6156d875ce2ba4a4d1db05c1b3803b48/00000000000-0000000000000000000000000000000000000000.1753fee34b3-af03a176909e1cf51646bb5232f580099d7d2fa0
# -rw-r--r-- 1 bch bch  191 Oct 19 10:16 repo/30/37/e9bc6156d875ce2ba4a4d1db05c1b3803b48/1753fee34b3-af03a176909e1cf51646bb5232f580099d7d2fa0.1753feedc25-1881686787e551ad54b580f395088887e7152723
# -rw-r--r-- 1 bch bch   39 Oct 19 10:16 repo/75/71/49eedbfc5eab5864122c80a7154f3adcf701
# -rw-r--r-- 1 bch bch 686K Oct 19 10:16 repo/af/14/00ce0ff1f8d4cf268cfdeaa777a3341f1d43
# -rw-r--r-- 1 bch bch  155 Oct 19 10:16 repo/b8/41/2543a21bceaefbf539bb0b51d7590b563566
# -rw-r--r-- 1 bch bch  15K Oct 19 10:16 repo/e6/0c/0ea648ae8ae4a717f29622eb75a1a4029b6e
