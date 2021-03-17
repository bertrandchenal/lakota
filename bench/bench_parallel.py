# This script shows the effect of IO lag when writing series with and
# without parallism enabled.

# ** output **
# 0.001-False 819.51ms
# 0.001-True 788.83ms
# 0.01-False 1.07s
# 0.01-True 828.28ms
# 0.1-False 3.64s
# 0.1-True 1.47s


from time import sleep

from numpy import arange, sin

from lakota import Repo, Schema
from lakota.pod import MemPOD
from lakota.utils import settings, timeit

SIZE = 1_000_000
cols = list("abcdefg")
schema = Schema(key="int*", **{x: "float" for x in cols})
frm = {
    "key": range(SIZE),
}
for x in cols:
    frm[x] = sin(arange(SIZE))


# Simulate network lag
def lag(fn, delay):
    def wrapper(*a, **kw):
        sleep(delay)
        return fn(*a, **kw)

    return wrapper


mempod_write = MemPOD.write

for delay in (0.001, 0.01, 0.1):
    MemPOD.write = lag(mempod_write, delay)
    for threaded in (False, True):
        settings.threaded = threaded
        with timeit(f"{delay}-{threaded}"):
            repo = Repo()
            clc = repo.create_collection(schema, "clc")
            with clc.multi():
                for name in "ABC":
                    series = clc / name
                    series.write(frm)
