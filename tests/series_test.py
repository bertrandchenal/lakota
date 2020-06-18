from baltic import POD, Schema, Segment, Series


def test_write_series():
    pod = POD.from_uri("memory://")
    schema = Schema(["timestamp:int", "value:float"])
    series = Series(schema, pod)

    # Write some values
    sgm = Segment.from_df(
        schema,
        {"timestamp": [1589455903, 1589455904, 1589455905], "value": [1.1, 2.2, 3.3],},
    )
    series.write(sgm)

    # Read those back
    sgm_copy = series.read()
    assert sgm_copy == sgm
