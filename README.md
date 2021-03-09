

# Lakota

Inspired by Git, Lakota is a version-control system for numerical
series. It is meant to be used on top of S3, on the local filesystem
or in memory.

Documentation: https://bertrandchenal.github.io/lakota/

# Quickstart

The following script will create a timeseries on a repository backed
by a local folder and read it back.

``` python
from lakota import Repo, Schema

ts_schema = Schema(timestamp="timestamp*", value="float")
repo = Repo("my-data-folder")  # or Repo("s3:///my-s3-bucket")
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


# Cookbook

See the [cookbook](cookbook folder) for more examples.


# Roadmap / Ideas

- Selective suppression of past revisions
- Shallow clone
