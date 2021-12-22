
# Multiple writes, revision history and defrag

This example starts by accumulating some writes in different series in
a collection. It then shows how to see all the revisions of the
collection, how to read historical data and how to remove past
revisions.


```python
from lakota import Repo, Schema

ts_schema = Schema(timestamp="timestamp*", value="float")
repo = Repo() # in-memory repo
clct = repo.create_collection(ts_schema, "temperature")

# Let's accumulate different writes
for city in ('Brussels', 'Paris', 'London'):
    for i in range(1, 10):
        series = clct.series(city)
        df = {
            "timestamp": [
                f"2020-01-{i+j:02}" for j in range(7)
            ],
            "value": [i] * 7,
        }
        series.write(df)

df = clct.series('Brussels').df()
print(df)
# ->
#     timestamp  value
# 0  2020-01-01    1.0
# 1  2020-01-02    2.0
# 2  2020-01-03    3.0
# 3  2020-01-04    4.0
# 4  2020-01-05    5.0
# 5  2020-01-06    6.0
# 6  2020-01-07    7.0
# 7  2020-01-08    8.0
# 8  2020-01-09    9.0
# 9  2020-01-10    9.0
# 10 2020-01-11    9.0
# 11 2020-01-12    9.0
# 12 2020-01-13    9.0
# 13 2020-01-14    9.0
# 14 2020-01-15    9.0


# We access the different revisions
print(clct.changelog.log())
# ->
# [<Revision 0000000000-0000000000000000000000000000000000000000.178163e84e3-bb63d6e7f413c5bde452bdb0d081053511bcb009 >,
# ... 25 intermediate revisions ...
# <Revision 178163e84f5-3cfcabd67503222d157f0cb4f81d79aa0df142d6.178163e84f6-8a717abd1b24878c28adf44f531c33c96ebd3ed1 *>]


# We can for example read a series as of a given time in the past
past_revision = clct.changelog.log()[5]
print(past_revision)
print(clct.series('Brussels').df(before=past_revision.epoch))
# ->
#     timestamp  value
# 0  2020-01-01    1.0
# 1  2020-01-02    2.0
# 2  2020-01-03    3.0
# 3  2020-01-04    4.0
# 4  2020-01-05    5.0
# 5  2020-01-06    5.0
# 6  2020-01-07    5.0
# 7  2020-01-08    5.0
# 8  2020-01-09    5.0
# 9  2020-01-10    5.0
# 10 2020-01-11    5.0



# Finally we use defrag to combine all the revisions into one and remove the history
clct.defrag()
print(clct.changelog.log())
# [<Revision 00000000000-0000000000000000000000000000000000000000.1781641025f-bafbe905f65ac476e49b3d2357b3519ed984d528 *>]
```
