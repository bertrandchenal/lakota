from itertools import islice

from pytest import raises

from jensen import Registry, Schema, Frame

labels = "zero one two three four five six seven eight nine".split()


def test_create_labels(pod):
    """
    Create all labels in one go
    """
    reg = Registry(pod=pod)
    schema = Schema(["timestamp:int", "value:float"])
    reg.create(schema, *labels)

    # Test that we can get back those series
    for label in labels:
        series = reg.get(label)
        assert series.schema == schema

    # Same after packing
    reg.schema_series.changelog.pack()
    for label in labels:
        series = reg.get(label)
        assert series.schema == schema


def test_create_labels_chunks(pod):
    """
    Create all labels in chunks
    """
    it = iter(labels)
    reg = Registry(pod=pod)
    schema = Schema(["timestamp:int", "value:float"])
    while True:
        sl_labels = list(islice(it, 3))
        if not sl_labels:
            break
        reg.create(schema, *sl_labels)

    # Test that we can get back those series
    for label in labels:
        series = reg.get(label)
        assert series.schema == schema

    # Same after packing
    reg.schema_series.changelog.pack()
    for label in labels:
        series = reg.get(label)
        assert series.schema == schema

    # Same after sqash
    reg.squash()
    for label in labels:
        series = reg.get(label)
        assert series.schema == schema


def test_delete(pod):
    reg = Registry(pod=pod)
    schema = Schema(["timestamp:int", "value:float"])
    reg.create(schema, *labels)
    expected = sorted(labels)
    assert list(reg.search()["label"]) == expected

    # Remove a label and check result
    reg.delete("nine", "zero")
    assert list(reg.search()["label"]) == [
        l for l in expected if l not in ("nine", "zero")
    ]


def test_pull():
    schema = Schema(["timestamp:int", "value:float"])
    label = "LABEL"
    remote_reg = Registry()
    remote_reg.create(schema, label)
    rseries = remote_reg.get(label)
    for i in range(10):
        rseries.write(
            {"timestamp": range(i, i + 10), "value": range(i + 100, i + 110),}
        )
    expected = rseries.read()

    # Test pull
    local_reg = Registry()
    local_reg.pull(remote_reg, label)
    lseries = local_reg.get(label)
    assert lseries.read() == expected

    # Test push
    other_reg = Registry()
    remote_reg.push(other_reg, label)
    oseries = other_reg.get(label)
    assert oseries.read() == expected

    # Test with existing series
    local_reg = Registry()
    local_reg.create(schema, label)
    local_reg.pull(remote_reg, label)
    lseries = other_reg.get(label)
    assert oseries.read() == expected

    # Test with existing series with existing data
    local_reg = Registry()
    lseries = local_reg.create(schema, label)
    frm = Frame(schema, {"timestamp": range(0, 20), "value": range(10, 20),})
    lseries.write(frm)
    local_reg.pull(remote_reg, label)
    assert lseries.read() == frm

    # Test with existing series with other schema
    local_reg = Registry()
    other_schema = Schema(["timestamp:int", "value:int"])
    lseries = local_reg.create(other_schema, label)
    with raises(ValueError):
        local_reg.pull(remote_reg, label)


def test_gc(pod):
    schema = Schema(["timestamp:int", "value:float"])
    reg = Registry(pod=pod)
    reg.create(schema, "label_a", "label_b")
    for offset, label in enumerate(("label_a", "label_b")):
        series = reg.get(label)
        for i in range(offset, offset + 10):
            series.write(
                {"timestamp": range(i, i + 10), "value": range(i + 100, i + 110),}
            )

    # Squash label_a
    series = reg.get("label_a")
    series.squash()

    # Launch garbage collection
    count = reg.gc()

    # Count must be 2 because the two series are identical except for
    # two data frames (the last write is offseted and contains two
    # columns)
    assert count == 2
