
# Caching

Caching can be enabled by instantiating a Repo object combining
several storage locations:

``` python
import os
from lakota import Repo, Schema
from lakota.utils import logger


# Instantiate a remote repo and populate it
remote_repo = Repo('/tmp/remote_repo')
schema = Schema(timestamp='timestamp*', value='float')
clct = remote_repo.create_collection(schema, "temperature")
timestamp = [
    "2020-01-01T00:00",
    "2020-01-02T00:00",
    "2020-01-03T00:00",
    "2020-01-04T00:00",
]

# Create a series
series = clct / 'Brussels'
series.write({
    'timestamp': timestamp,
    'value': range(4),
})


# Use memory as cache
cached_repo = Repo(['memory://', '/tmp/remote_repo'])
series = cached_repo / 'temperature' / 'Brussels'
print(series.df())
# ->
#    timestamp  value
# 0 2020-01-01    0.0
# 1 2020-01-02    1.0
# 2 2020-01-03    2.0
# 3 2020-01-04    3.0


# To illustrate caching behavior, we "destroy" the source repo
os.rename('/tmp/remote_repo', '/tmp/remote_repo_bis')
# Reading the data wont work, the cache still contains data, but rely
# on remote repo for listing it. So if a file is removed from remote
# it wont be read from local.
series = cached_repo / 'temperature' / 'Brussels'
print(series.df())
# ->
# Empty DataFrame
# Columns: [timestamp, value]
# Index: []

# Yet the data is there, we cheat by accessing the in-memory data on
# its own:
repo = Repo(pod=cached_repo.pod.local)
series = repo / 'temperature' / 'Brussels'
print(series.df())
# ->
#    timestamp  value
# 0 2020-01-01    0.0
# 1 2020-01-02    1.0
# 2 2020-01-03    2.0
# 3 2020-01-04    3.0

# If we "fix" the source repo, our cache works again. This time we
# activate logging to show that with a warm cache the remote repo is
# only used for listing the changelog of the collection:
os.rename('/tmp/remote_repo_bis', '/tmp/remote_repo')
series = cached_repo / 'temperature' / 'Brussels'
logger.setLevel('DEBUG')
print(series.df())
# ->
# DEBUG:2020-12-18 14:34:19: LIST /tmp/remote_repo/70/33/e90fcdda169d2f5d08da17507b0c5db52029 .
# DEBUG:2020-12-18 14:34:19: READ memory://70/33/e90fcdda169d2f5d08da17507b0c5db52029 00000000000-0000000000000000000000000000000000000000.176760ef1f5-efd4198272b4a671db506c360ce036e1d28c2efb
# DEBUG:2020-12-18 14:34:19: READ memory://e0/62 63beda1a1c2656d1089d3cb4d39d98eed342
# DEBUG:2020-12-18 14:34:19: READ memory://39/95 0a757393aea125b8018395c48fa5279e2753
#    timestamp  value
# 0 2020-01-01    0.0
# 1 2020-01-02    1.0
# 2 2020-01-03    2.0
# 3 2020-01-04    3.0
```
