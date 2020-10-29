import pytest
from numpy import array
from pandas import DataFrame

from lakota import Frame, Repo, Schema

NAMES = list("abcde")
VALUES = [1.1, 2.2, 3.3, 4.4, 5.5]
base_schema = Schema(["category str*", "value float"])


@pytest.fixture
def frame_values():
    return {
        "value": VALUES,
        "category": NAMES,
    }


@pytest.fixture
def frm(frame_values):
    frm = Frame(base_schema, frame_values)
    return frm


def test_index_slice():
    schema = Schema(["x int*"])
    frm = Frame(schema, {"x": [1, 2, 3, 4, 5, 5, 5, 6]})

    # include both side
    res = frm.index_slice([2], [4], closed="both")["x"]
    assert all(res == [2, 3, 4])

    # include only left
    res = frm.index_slice([2], [4], closed="left")["x"]
    assert all(res == [2, 3])

    # include only right
    res = frm.index_slice([2], [4], closed="right")["x"]
    assert all(res == [3, 4])

    # implict right
    res = frm.index_slice([5], [5], closed="both")["x"]
    assert all(res == [5, 5, 5])

    res = frm.index_slice([1], [1], closed="both")["x"]
    assert all(res == [1])

    res = frm.index_slice([6], [6], closed="both")["x"]
    assert all(res == [6])


def test_getitem():
    # with a slice
    schema = Schema(["x int*"])
    frm = Frame(schema, {"x": [1, 2, 3, 4, 5, 5, 5, 6]})
    frm2 = frm[5:]
    assert all(frm2["x"] == [5, 5, 5, 6])

    # with a mask
    frm2 = frm[array([True, False] * 4)]
    assert all(frm2["x"] == [1, 3, 5, 5])


def test_mask():
    # with an array
    schema = Schema(["x int*"])
    frm = Frame(schema, {"x": [1, 2, 3, 4, 5, 5, 5, 6]})
    frm2 = frm.mask(array([True, False] * 4))
    assert all(frm2["x"] == [1, 3, 5, 5])

    # with an expression
    frm2 = frm.mask("(= (% self.x 2) 0")
    assert all(frm2["x"] == [2, 4, 6])


def test_double_slice(frame_values, frm):
    # in-meory frame
    frm = frm.slice(1, None).slice(None, 2)
    assert all(frm["value"] == VALUES[1:][:2])

    # frame created from repo
    collection = Repo().create_collection(frm.schema, "collection")
    series = collection / "my-label"
    series.write(frame_values)
    frm = series.frame()
    frm = frm.slice(1, None).slice(None, 2)
    assert all(frm["value"] == VALUES[1:][:2])


def test_reduce_agg():
    schema = Schema(
        f"""
        timestamp timestamp*
        category str*
        value int
        """
    )
    values = {
        "timestamp": [1589455901, 1589455901, 1589455902, 1589455902],
        "category": list("abab"),
        "value": [1, 2, 3, 4],
    }

    frm = Frame(schema, values)
    for op in ("sum", "min", "max", "first", "last", "mean"):
        new_frm = frm.reduce(category="category", value=f"({op} self.value)")
        if op == "min":
            assert list(new_frm["value"]) == [1, 2]
        elif op == "max":
            assert list(new_frm["value"]) == [3, 4]
        elif op == "sum":
            assert list(new_frm["value"]) == [4, 6]
        elif op == "mean":
            assert list(new_frm["value"]) == [2, 3]
        elif op == "first":
            assert list(new_frm["value"]) == [1, 2]
        elif op == "last":
            assert list(new_frm["value"]) == [3, 4]
        else:
            raise

    for op in ("sum", "min", "max", "first", "last", "mean"):
        new_frm = frm.reduce(
            timestamp='(floor self.timestamp "D")', value=f"({op} self.value)"
        )
        if op == "min":
            assert list(new_frm["value"]) == [1]
        elif op == "max":
            assert list(new_frm["value"]) == [4]
        elif op == "sum":
            assert list(new_frm["value"]) == [10]
        elif op == "mean":
            assert list(new_frm["value"]) == [2.5]
        elif op == "first":
            assert list(new_frm["value"]) == [1]
        elif op == "last":
            assert list(new_frm["value"]) == [4]
        else:
            raise

def test_reduce_without_agg():
    schema = Schema(
        f"""
        timestamp timestamp*
        category str*
        value int
        """
    )
    values = {
        "timestamp": [1589455901, 1589455901, 1589455902, 1589455902],
        "category": list("abab"),
        "value": [1, 2, 3, 4],
    }

    frm = Frame(schema, values)
    # No changes to column
    assert frm == frm.reduce(timestamp='timestamp', category='category', value='value')
    # Mapping on one column
    res = frm.reduce(value='(% self.value 2)')['value']
    assert list(res) == [1, 0, 1, 0]

    # Mapping over two columns
    expected = frm['timestamp'] + frm['value']
    new_frm = frm.reduce(new_col='(+ self.value self.timestamp)')
    assert all(new_frm['new_col'] == expected)


def test_concat(frm):
    frm2 = Frame.concat(frm, frm)
    for name in frm:
        col = list(frm[name])
        expected = sorted(col + col)
        result = list(frm2[name])
        assert result == expected

    assert Frame.concat(frm) == frm
    assert Frame.concat() is None


def test_eq(frm):
    assert (frm == frm) is True


def test_df_conversion():
    df = DataFrame({
        "category": NAMES,
        "value": VALUES,
    })
    # Convert to lakota frame and back to df
    frm = Frame(base_schema, df)
    for col in frm:
        assert all(frm.df()[col] == df[col])
