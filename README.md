

# Lakota

Inspired by Git, Lakota is a version-control system for numerical
series. It is meant to be used on top of S3, on the local filesystem
or in memory.


# Quickstart

The following script will create a timeseries on a registry backed by
a local folder and read it back.

``` python
from lakota import Repo, Schema

ts_schema = Schema(["timestamp timestamp*", "value float"])
repo = Repo("my-data-folder")  # or Repo("s3://my-s3-bucket")
clct = repo.create_collection(ts_schema, "temperature")
series = clct.series('Brussels')
df = {
    "timestamp": [
        "2020-01-01T00:00",
        "2020-01-02T00:00",
        "2020-01-03T00:00",
        "2020-01-04T00:00",
    ],
    "value": [1, 2, 3, 4],
}

series.write(df)
df = series[:'2020-01-03'].df()
print(df)
# ->
#    timestamp  value
# 0 2020-01-01    1.0
# 1 2020-01-02    2.0
# 2 2020-01-03    3.0
```

Let's try something more complex based on the WHO COVID dataset, we
first load some data in Lakota:

``` python
from lakota import Repo, Schema
from requests import get
from pandas import read_csv

# Instantiate a local repo and define a schema
repo = Repo('file://db')
schema = Schema('''
date_reported timestamp*
new_cases int
cumulative_cases int
new_deaths int
cumulative_deaths int
''')

# Download csv from WHO's website
resp = get('https://covid19.who.int/WHO-COVID-19-global-data.csv', stream=True)
resp.raw.decode_content=True
df = read_csv(resp.raw)
rename = {c: c.strip().lower() for c in df.columns}
df = df.rename(columns=rename)

# Create one series per country
clct = repo.create_collection(schema, 'covid')
for key, sub_df in df.groupby(['who_region', 'country']):
    sub_df = sub_df.sort_values(by='date_reported')
    who_region, country = key
    series = clct / {who_region}_{country}'
    series.write(sub_df)
```

Then we can read some data back:

``` python
from lakota import Repo

repo = Repo('db')
series = repo / 'covid' / 'EURO_Belgium'

start = '2020-08-20T00:00:00'
stop = '2020-08-30T00:00:00'
df = series[start:stop].df()
print(df)
# ->
#   date_reported  new_cases  cumulative_cases  new_deaths  cumulative_deaths
# 0    2020-08-20        612             80868           9               9854
# 1    2020-08-21        581             81449           4               9858
# 2    2020-08-22        512             81961           3               9861
# 3    2020-08-23        207             82168           5               9866
# 4    2020-08-24        116             82284           3               9869
# 5    2020-08-25        610             82894           5               9874
# 6    2020-08-26        524             83418           5               9879
# 7    2020-08-27        521             83939           4               9883
# 8    2020-08-28        520             84459           3               9886
# 9    2020-08-29        445             84904           3               9889
```
