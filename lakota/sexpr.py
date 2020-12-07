import operator
import shlex
from functools import reduce

import numpy
from numpy import bincount, inf, max, maximum, mean, min, minimum, quantile, repeat, sum

UNSET = object()


def list_to_dict(*items):
    it = iter(items)
    return dict(zip(it, it))


class KWargs:
    def __init__(self, *items):
        self.value = list_to_dict(*items)


class Env:
    def __init__(self, values):
        if isinstance(values, Env):
            # Un-wrap env
            values = values.values
        self.values = values

    def get(self, key, default=UNSET):
        res = self.values
        for part in key.split("."):
            try:
                res = getattr(res, part)
            except AttributeError:
                pass
            try:
                res = res[part]
            except KeyError:
                if default is not UNSET:
                    return default
                raise
        return res


class Token:
    def __init__(self, value):
        self.value = value

    def as_string(self):
        res = self.value.strip("'\"")
        if len(res) < len(self.value):
            return res
        return None

    def __repr__(self):
        return f"<Token {self.value}>"

    def as_number(self):
        try:
            return int(self.value)
        except ValueError:
            pass

        try:
            return float(self.value)
        except ValueError:
            pass

        return None

    def is_aggregate(self):
        return self.value in AST.aggregates

    def eval(self, env):
        # Eval builtins
        if self.value in AST.builtins:
            return AST.builtins[self.value]
        # Eval aggregates
        if self.is_aggregate():
            return Agg(self.value, env)
        # Eval floats and int
        res = self.as_number()
        if res is not None:
            return res
        # Eval strings
        res = self.as_string()
        if res is not None:
            return res
        # Eval env
        try:
            return env.get(self.value)
        except KeyError:
            pass
        # Eval numpy function
        fn = getattr(numpy, self.value, None)
        if fn:
            return fn

        raise ValueError(f'Unexpected token: "{self.value}"')


class Agg:
    def __init__(self, op, env):
        self.op = op
        self.env = env

    def __call__(self, arr=None, *operands):
        bins = self.env.get("_bins", None)
        keys = self.env.get("_keys", None)
        if bins is not None:
            return self.binned(arr, bins, keys, operands=operands)
        return self.plain(arr, operands=operands)

    def plain(self, arr, operands=None):
        """
        Plain aggregates
        """
        if self.op == "max":
            return max(arr)
        elif self.op == "min":
            return min(arr)
        elif self.op == "mean":
            return mean(arr)
        elif self.op == "sum":
            return sum(arr)
        elif self.op == "quantile":
            qt = operands[0] if len(operands) > 0 else 0.5
            interpolation = operands[1] if len(operands) > 1 else "linear"
            return quantile(arr, qt, interpolation=interpolation)
        raise ValueError(f'Aggregation "{self.op}" is not supported')

    def binned(self, arr, bins, keys):
        if self.op == "sum":
            return bincount(bins, weights=arr)
        elif self.op in ("mean", "average"):
            res = bincount(bins, weights=arr)
            counts = bincount(bins)
            return res / counts
        elif self.op == "max":
            res = repeat(-inf, len(keys))
            maximum.at(res, bins, arr)
            return res
        elif self.op == "min":
            res = repeat(inf, len(keys))
            minimum.at(res, bins, arr)
            return res
        elif self.op == "last":
            res = repeat(None, len(keys))
            # Repeated index keep last value
            res[bins] = arr
            return res
        elif self.op == "first":
            res = repeat(None, len(keys))
            # Repeated (revsersed) index gives first value
            res[bins[::-1]] = arr[::-1]
            return res
        elif self.op in ("count", "len"):
            return bincount(bins)

        raise ValueError(f'Aggregation "{self.op}" is not supported')


def tokenize(expr):
    lexer = shlex.shlex(expr)
    lexer.wordchars += ".!=<>:{}-"
    for i in lexer:
        yield Token(i)


def scan(tokens, end_tk=")"):
    res = []
    for tk in tokens:
        if tk.value == end_tk:
            return res
        elif tk.value == "(":
            res.append(scan(tokens))
        else:
            res.append(tk)

    tail = next(tokens, None)
    if tail:
        raise ValueError(f'Unexpected token: "{tail.value}"')
    return res


class AST:
    builtins = {
        "true": True,
        "false": False,
        "+": lambda *x: reduce(operator.add, x),
        "-": lambda *x: reduce(operator.sub, x),
        "*": lambda *x: reduce(operator.mul, x),
        "/": lambda *x: reduce(operator.truediv, x),
        "%": lambda *x: reduce(operator.mod, x),
        "and": lambda *x: reduce(operator.and_, x),
        "or": lambda *x: reduce(operator.or_, x),
        "<": lambda *x: reduce(operator.lt, x),
        "<=": lambda *x: reduce(operator.le, x),
        "=": lambda *x: reduce(operator.eq, x),
        "!=": lambda *x: reduce(operator.ne, x),
        ">=": lambda *x: reduce(operator.ge, x),
        ">": lambda *x: reduce(operator.gt, x),
        "~": lambda *xs: all(not x for x in xs),
        "in": lambda *x: x[0] in x[1:],
        "list": lambda *x: list(x),
        "dict": list_to_dict,
        "kw": KWargs,
    }

    aggregates = {
        "min",
        "max",
        "sum",
        "first",
        "last",
        "mean",
        "average",
        "quantile",
        "count",
        "len",
    }

    def __init__(self, tokens):
        self.tokens = tokens

    @classmethod
    def parse(cls, expr):
        res = tokenize(expr)
        tokens = scan(res)[0]
        return AST(tokens)

    def eval(self, env=None):
        env = Env(env or {})
        if isinstance(self.tokens, Token):
            return self.tokens.eval(env)
        head, tail = self.tokens[0], self.tokens[1:]
        args = [AST(tk).eval(env) for tk in tail]

        # Split normal and kw args
        simple_args = []
        kw_args = {}
        for a in args:
            if isinstance(a, KWargs):
                kw_args.update(a.value)
            else:
                simple_args.append(a)

        fn = head.eval(env)
        return fn(*simple_args, **kw_args)

    def is_aggregate(self):
        for tk in self.tokens:
            if isinstance(tk, Token):
                if tk.value in self.aggregates:
                    return True
            elif isinstance(tk, list):
                if any(t.is_aggregate() for t in tk):
                    return True
            elif tk.is_aggregate():
                return True
        return False
