
# Baltic

Inspired by Git, Baltic is a version-control system for numerical
series. It is meant to be used on top of S3, on the local filesystem
or in memory.

# Quickstart

``` python

>>> from baltic import Registry, Schema
>>> ts_schema = Schema(['timestamp:timestamp', 'value:float'])
>>> reg = Registry('my-data-folder')
>>> reg.create(ts_schema, 'my-timeseries')
[<baltic.series.Series object at 0x7f48d0702390>]
>>> series = reg.get('my-timeseries')
>>>
>>> df = {
...     'timestamp': ['2020-01-01T00:00', '2020-01-02T00:00', '2020-01-03T00:00', '2020-01-04T00:00'],
...     'value': [1, 2, 3, 4],
... }
>>>
>>> series.write(df)
'0000000000000000000000000000000000000000.173c45d792a-306686bb001cfcaae3f8af9943e2945c9711ad5c'
>>> series.read()
<baltic.frame.Frame object at 0x7f48d0702cf8>
>>>
>>> frame = series.read()
>>> frame['timestamp']
array(['2020-01-01T00:00:00', '2020-01-02T00:00:00',
       '2020-01-03T00:00:00', '2020-01-04T00:00:00'],
      dtype='datetime64[s]')
>>> frame['value']
array([1., 2., 3., 4.])
```
