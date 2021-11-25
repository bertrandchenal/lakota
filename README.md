

# Lakota

Inspired by Git, Lakota is a version-control system for numerical
series. It is meant to be used on top of S3, on the local filesystem
or in memory.

Documentation: https://bertrandchenal.github.io/lakota/

## Install

Lakota requires Python 3.7 or later, the `s3fs` module is required if
you want to access datasets on S3.

```
pip install lakota s3fs
```

Python 3.6 should also work with the addition of the `dataclasses`
module and an older version of `s3fs`:

```
pip install dataclasses lakota "s3fs<0.5"
```


## Quickstart

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
df = series.df(stop='2020-01-03')
print(df)
# ->
#    timestamp  value
# 0 2020-01-01    1.0
# 1 2020-01-02    2.0
# 2 2020-01-03    3.0
```

Note that thanks to the schema definition, data type conversion is
made automatically: The input `df` contains strings in the timestamp
column (and integers in the value column) that will be properly casted
before being saved.

The schema also defines which column(s) is the index of the series
thanks to the `*` in the type definition.


## Examples

See the [examples folder](https://github.com/bertrandchenal/lakota/tree/master/examples/)
for more examples.


## Compared to ...

### DBMS

Coming from a DBMS like Postgresql, Lakota gives you simple horizontal
scaling (thanks to S3, but Minio also provides clustering), while
keeping some concurrency control. It is also much faster for large
writes (but much slower for small writes) and usually comes with a
[disk usage 10x to 100x smaller](https://github.com/bertrandchenal/lakota/blob/master/bench/bench_pg.py).

### Filesystem

The fastest way to save a timeseries is to write a csv or parquet
file. And it's dead simple!

While this is indeed a good solution for static data, you will quickly
face some limitations if the data is updated periodically:

- A large series will generate an impractically large file. The
  solution is to chunk the series into a handful of files. How to size
  those? If the chunks are to big or too small, the performance will
  suffer. What if a timeseries grows dramatically?
- How to apply a partial update? Is it better to re-write a full chunk
  or keep an extra file to track updates?
- How to prevent or mitigate concurrent writes?
- What about: caching, change detection, file integrity, etc.


### REST API

In some use cases, Lakota provides an efficient alternative to REST
API: Think about situations when a database host a collection of
timeseries and a backend application is used to present a REST API in
front of it.

In those situations, the main pain point will be about serialization:
the backend has to de-serialize the data coming from the database and
then serialize it again into json. It can quickly become slow and
memory intensive.

Moreover, data-streaming over a REST api is not easy to implement
(something databases and database driver supports). So, if not
forbidden, large queries will at best jeopardize the performances and
a worst will result in a deny of service of the application server
itself by eating most of the host memory.

Lakota sidesteps those two issues because it consumes directly the
chunks of data without any intermediate conversions and benefits from
the builtin compression and caching.

