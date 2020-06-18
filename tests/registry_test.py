from baltic import Registry, Schema


def test_new_label():
    reg = Registry()
    schema = Schema(["timestamp:int", "value:float"])
    reg.create(schema, "temperature", "wind_speed")
    series = reg.get("temperature")
    assert series
    assert series.schema == schema
