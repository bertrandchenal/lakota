from numpy import asarray

from lakota.sexpr import AST, KWargs

trueish_expr = [
    "(true)",
    "(~ false)",
    "(= (- (+ 1 1) (+ 2 2)) -2)",
    "(= (* (/ 3 2) (/ 7 2)) 5.25)",
    "(= (/ 6 3 2) 1)",
    "(or false true false)",
    "(~ (and false true false))",
    '(in "foo" "ham" "foo" "bar")',
]


def test_trueish_expr():
    for code in trueish_expr:
        ast = AST.parse(code)
        res = ast.eval()
        assert res is True


def test_kw():
    res = AST.parse("(# 'return_counts' true)").eval()
    assert isinstance(res, KWargs)


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

    res = AST.parse("(unique arr (# 'return_counts' true))").eval({"arr": arr})
    assert all(res[0] == [1, 2])
    assert all(res[1] == [2, 2])


def test_some_expr_with_env():
    env = {
        "a": asarray([1, 1]),
        "b": asarray([2, 2]),
        "x-x": 1,
        "y_y": 2,
        "frame": {
            "u": 1,
            "v": 2,
        },
    }
    res = AST.parse("(+ a b)").eval(env)
    expected = asarray([3, 3])
    assert all(res == expected)

    ast = AST.parse("(+ x-x y_y)")
    assert ast.eval(env) == 3

    ast = AST.parse("(+ frame.u frame.v)")
    assert ast.eval(env) == 3


def test_only_litterals():
    res = AST.parse("(list 1 2 3)").eval()
    assert res == [1, 2, 3]

    res = AST.parse("(1)").eval()
    assert res == 1

    res = AST.parse('("spam")').eval()
    assert res == "spam"

    res = AST.parse('(dict "ham" 1 "spam" 2)').eval()
    assert res["ham"] == 1
    assert res["spam"] == 2
