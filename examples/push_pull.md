
# Push & Pull

The following example demonstrate how to push/pull data between
repositories or between collection.

``` python
from lakota import Repo, Schema

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

# Local repo is empty
local_repo = Repo('/tmp/local_repo')
print(local_repo.ls())
# -> []

# Pull everything from remote
local_repo.pull(remote_repo)
# List again
print(local_repo.ls())
# -> ['temperature']

# And read some values
series = local_repo / 'temperature' / 'Brussels'
print(series.df())
# ->
#    timestamp  value
# 0 2020-01-01    0.0
# 1 2020-01-02    1.0
# 2 2020-01-03    2.0
# 3 2020-01-04    3.0

# Create a new collection on local repo
clct = local_repo.create_collection(schema, "rainfall")
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

# Push that collection only, under another name
remote_clct = remote_repo.create_collection(schema, 'precipitation')
clct.push(remote_clct)

print(remote_clct.series('Brussels').df())
#  ->
#    timestamp  value
# 0 2020-01-01    0.0
# 1 2020-01-02    1.0
# 2 2020-01-03    2.0
# 3 2020-01-04    3.0
```

# Merging

In case of concurrent writes, the changelog (that keeps track of the
history of modification of a given timeseries) may diverge. In this
case the collection has to be merged.

``` python
from pprint import pprint
from lakota import Repo, Schema


# Instanciate a two repos and populate them
repo = Repo('/tmp/repo')
repo_bis = Repo('/tmp/repo_bis')
schema = Schema('''
timestamp timestamp*
value float
''')
clct = repo.create_collection(schema, "temperature")

# First push (with an empty collection)
repo.push(repo_bis)
clct_bis = repo_bis / 'temperature'
pprint(clct_bis.ls())
# -> []

# Create a series and write original repo
timestamp = [
    "2020-01-01T00:00",
    "2020-01-02T00:00",
    "2020-01-03T00:00",
    "2020-01-04T00:00",
]
series = clct / 'Brussels'
series.write({
    'timestamp': timestamp,
    'value': range(4),
})

# Let's write some data through clct_bis
timestamp = [
    "2020-01-02T00:00", # starts 1 day later !
    "2020-01-03T00:00",
    "2020-01-04T00:00",
    "2020-01-05T00:00",
]
series_bis = clct_bis / 'Brussels'
series_bis.write({
    'timestamp': timestamp,
    'value': range(10, 14), # Other values!
})


# And try to pull and read it with the original repo, the second write (on
# `series_bis`) overshadows the first on (on `series`):
repo.pull(repo_bis)
pprint(series.df())
# ->
#    timestamp  value
# 0 2020-01-02   10.0
# 1 2020-01-03   11.0
# 2 2020-01-04   12.0
# 3 2020-01-05   13.0

# But no data is lost, we have two heads in the changelog, a merge
# will fix the situation:
pprint(clct.changelog.log())
# ->
# [<Revision 00000000000-0000000000000000000000000000000000000000.176768e5f52-efd4198272b4a671db506c360ce036e1d28c2efb *>,
#  <Revision 00000000000-0000000000000000000000000000000000000000.176768e5f57-a7ea839500abe659fef815a7591104e948d98d13 *>]
clct.merge()
pprint(clct.changelog.log())
# ->
# [<Revision 00000000000-0000000000000000000000000000000000000000.176768ef53a-efd4198272b4a671db506c360ce036e1d28c2efb >,
#  <Revision 176768ef53a-efd4198272b4a671db506c360ce036e1d28c2efb.176768ef546-3494115c8959ec5dac132f1d10c658f4ab4dc8e4 *>,
#  <Revision 00000000000-0000000000000000000000000000000000000000.176768ef53e-a7ea839500abe659fef815a7591104e948d98d13 >,
#  <Revision 176768ef53e-a7ea839500abe659fef815a7591104e948d98d13.176768ef547-3494115c8959ec5dac132f1d10c658f4ab4dc8e4 *>]


# We see that the merge as created two revisions both pointing to the
# new head (`176768ef547-34941...`).
# And we see the effect of the merge, the 1st of January is back:p
print(series.df())
#  ->
#     timestamp  value
# 0 2020-01-01    0.0
# 1 2020-01-02   10.0
# 2 2020-01-03   11.0
# 3 2020-01-04   12.0
# 4 2020-01-05   13.0
```

In case of concurrent writes on the same part of the timeseries, the
last write wins. It's important to note that machine clock is
important here, if concurrent writes here-above were made through two
different machines with incorrect clock the "last write" would be the
one from the machine with a clock running ahead.
