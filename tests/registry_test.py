from baltic import Registry, Schema, Segment

def test_new_label():
    reg = Registry()
    schema = Schema(['timestamp:int', 'value:float'])
    reg.create(schema, 'temperature', 'wind_speed')
    series = reg.get('temperature')
    assert series

    # Write some values
    sgm = Segment.from_df(schema, {
        'timestamp': [1589455903, 1589455904, 1589455905],
        'value': [1.1, 2.2, 3.3],
    })
    series.write(sgm)


    # Read those back
    sgm_copy = series.read()
    assert sgm_copy == sgm
