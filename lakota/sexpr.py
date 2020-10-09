import operator
from functools import partial, reduce

import numpy
from pyparsing import (
    Forward,
    Group,
    ParseResults,
    QuotedString,
    Suppress,
    Word,
    alphanums,
    oneOf,
    pyparsing_common,
)

_getter_default = object()


def getter(obj, field, default=_getter_default):
    try:
        return obj[field]
    except KeyError:
        pass
    try:
        return getattr(obj, field)
    except AttributeError:
        if default != _getter_default:
            return default
        raise


def list_to_dict(items):
    it = iter(items)
    return dict(zip(it, it))


class KWargs:
    def __init__(self, items):
        self.value = list_to_dict(items)


class Token:
    def __init__(self, value):
        self.value = value[0] if len(value) == 1 else value

    def __repr__(self):
        return f"<{type(self).__name__} {self.value}>"


class Identifier(Token):
    def eval(self, env):
        head, *tail = self.value.split(".")
        res = getter(env, head)
        for item in tail:
            res = getter(res, item)
        return res


class Bool(Token):
    def eval(self, env):
        return self.value == "true"


class Litteral(Token):
    def eval(self, env):
        return self.value


class Operator(Token):
    op_table = {
        "+": partial(reduce, operator.add),
        "-": partial(reduce, operator.sub),
        "*": partial(reduce, operator.mul),
        "/": partial(reduce, operator.truediv),
        "and": partial(reduce, operator.and_),
        "or": partial(reduce, operator.or_),
        "<": partial(reduce, operator.lt),
        "<=": partial(reduce, operator.le),
        "=": partial(reduce, operator.eq),
        "!=": partial(reduce, operator.ne),
        ">=": partial(reduce, operator.ge),
        ">": partial(reduce, operator.gt),
        "~": lambda xs: all(not x for x in xs),
        "in": lambda x: x[0] in x[1:],
        "list": list,
        "dict": list_to_dict,
        "#": KWargs,
    }

    def eval(self, tokens, env):
        op = self.op_table[self.value]
        return op(tokens)


class AST:
    def __init__(self, tokens):
        self.tokens = tokens

    @classmethod
    def parse(cls, code):
        tokens = sexp.parseString(code)[0]
        return AST(tokens)

    def eval_token(self, tk, env):
        if isinstance(tk, Token):
            value = tk.eval(env)
        elif isinstance(tk, (list, ParseResults)):
            value = AST(tk).eval(env)
        else:
            raise ValueError(f"unexpected item: {tk}")
        return value

    def eval(self, env=None):
        env = env or {}
        args = []
        # Eval args
        for a in self.tokens[1:]:
            value = self.eval_token(a, env)
            args.append(value)
        # Eval operator
        head = self.tokens[0]
        if isinstance(head, Operator):
            return head.eval(args, env)
        elif isinstance(head, Identifier):
            ident = head.value
            fn = getter(env, ident, None)
            if fn is None:
                fn = getattr(numpy, ident, None)
            if fn is None:
                raise NameError(f"{ident} is not defined")
            if not callable(fn):
                raise TypeError(f"{ident} is not callable")

            # Extract kwargs & execute
            kw = [a for a in args if isinstance(a, KWargs)]
            args = [a for a in args if not isinstance(a, KWargs)]
            if kw:
                kw, *other_kw = [k.value for k in kw]
                for okw in other_kw:
                    kw.update(okw)
                return fn(*args, **kw)
            return fn(*args)

        if len(self.tokens) > 1:
            values = " ".join(str(t.value) for t in self.tokens[1:])
            raise ValueError(f"Unexpected items: {values}")
        return self.eval_token(head, env)


# define grammar
LPAR, RPAR, LBRK, RBRK = (Suppress(c) for c in "()[]")
number = pyparsing_common.number.setParseAction(Litteral)
identifier = Word(alphanums, alphanums + "_-.:")
op = oneOf(" ".join(Operator.op_table))
bools = oneOf("true false").setParseAction(Bool)
string_ = QuotedString('"') | QuotedString("'")
item = (
    number
    | op.setParseAction(Operator)
    | bools
    | identifier.setParseAction(Identifier)
    | string_.setParseAction(Litteral)
)

sexp = Forward()
sexpList = Group(LPAR + sexp[...] + RPAR)
sexp <<= item | sexpList
