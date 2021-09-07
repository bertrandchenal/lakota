import pytest
from numpy import asarray

from lakota import Frame, Schema
from lakota.sexpr import AST, KWargs
from lakota.utils import floor

trueish_expr = [
    "true",
    "(~ false)",
    "(= (- (+ 1 1) (+ 2 2)) -2)",
    "(= (* (/ 3 2) (/ 7 2)) 5.25)",
    "(= (/ 6 3 2) 1)",
    "(or false true false)",
    "(~ (and false true false))",
    '(in "foo" (list "ham" "foo" "bar"))',
]
schema = Schema(timestamp="timestamp*", value="int")
values = {
    "timestamp": ["2020-01-01T11:30", "2020-01-02T12:30", "2020-01-03T13:30"],
    "value": [1, 2, 3],
}


def test_trueish_expr():
    for expr in trueish_expr:
        ast = AST.parse(expr)
        res = ast.eval()
        assert res is True


def test_kw():
    res = AST.parse("(kw 'return_counts' true)").eval()
    assert isinstance(res, KWargs)


def test_env():
    res = AST.parse("hello").eval(env={"hello": "world"})
    assert res == "world"


def test_numpy_fun():
    res = AST.parse("(asarray (list 1 2 3))").eval()
    assert all(res == asarray([1, 2, 3]))

    res = AST.parse("(max (list 1 2 3))").eval()
    assert res == 3

    arr = asarray([1, 2, 1, 2])
    # First arg of unique must be an array, the second one is "return_index"
    res = AST.parse("(unique arr true)").eval({"arr": arr})
    assert all(res[0] == [1, 2])
    assert all(res[1] == [0, 1])

    res = AST.parse("(unique arr (kw 'return_counts' true))").eval({"arr": arr})
    assert all(res[0] == [1, 2])
    assert all(res[1] == [2, 2])

    res = AST.parse("(char.lower arr)").eval({"arr": ["HAM", "Spam"]})
    assert all(res == ["ham", "spam"])


def test_with_frame():
    frm = Frame(schema, values)
    env = {"frm": frm, "floor": floor}
    res = AST.parse("(floor frm.timestamp 'Y')").eval(env)
    expect = asarray(["2020", "2020", "2020"], dtype="datetime64[Y]")
    assert all(res == expect)

    res = AST.parse("(floor frm.timestamp 'h')").eval(env)
    expect = asarray(
        ["2020-01-01T11", "2020-01-02T12", "2020-01-03T13"], dtype="datetime64"
    )
    assert all(res == expect)


def test_some_expr_with_env():
    env = {
        "a": asarray([1, 1]),
        "b": asarray([2, 2]),
        "x_x": 1,
        "y_y": 2,
        "frame": {
            "u": 1,
            "v": 2,
        },
    }
    res = AST.parse("(+ a b)").eval(env)
    expected = asarray([3, 3])
    assert all(res == expected)

    ast = AST.parse("(+ x_x y_y)")
    assert ast.eval(env) == 3

    ast = AST.parse("(+ frame.u frame.v)")
    assert ast.eval(env) == 3


def test_only_litterals():
    res = AST.parse("(list 1 2 3)").eval()
    assert res == [1, 2, 3]

    res = AST.parse("1").eval()
    assert res == 1

    res = AST.parse('"spam"').eval()
    assert res == "spam"

    res = AST.parse('(dict "ham" 1 "spam" 2)').eval()
    assert res["ham"] == 1
    assert res["spam"] == 2


def test_pathologic_inputs():
    exprs = [
        "(true)",
        "(1)",
        "(1",
        "(bar spam)",
    ]
    for expr in exprs:
        with pytest.raises(Exception):
            AST.parse(expr).eval()


def test_alias():
    res = AST.parse("(as (asarray (list 1 2 3)) 'new_name')").eval()
    arr = res.value
    alias = res.name
    assert all(arr == asarray([1, 2, 3]))
    assert alias == "new_name"

    frm = Frame(schema, values)
    frm = frm.reduce("(as self.timestamp 'ts')")
    assert all(frm["ts"] == asarray(values["timestamp"], "M"))

    # Test with custom env
    frm = Frame(schema, values)
    frm.env.update({"spammer": lambda arr, val: asarray([val] * len(arr))})
    frm = frm.reduce("(as (spammer self.timestamp 'SPAM') 'ts')")
    assert all(frm["ts"] == "SPAM")


def test_extract_tokens():
    res = AST.parse("(+ self.ham (- self.spam self.foo))")
    tokens = res.extract_tokens()
    values = [t.value for t in tokens]
    assert values == ["+", "self.ham", "-", "self.spam", "self.foo"]

    res = AST.parse("(self.foo)")
    tokens = res.extract_tokens()
    values = [t.value for t in tokens]
    assert values == ["self.foo"]
