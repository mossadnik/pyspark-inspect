from dataclasses import dataclass

from pyspark_inspect.term import *


@dataclass(frozen=True)
class Binop(Term):
    op: str = param()
    left: Term = child()
    right: Term = child()


@dataclass(frozen=True)
class Var(Term):
    name: str = param()


def test_fields():
    expr = Binop('+', Var('a'), Var('b'))
    assert expr.params == {'op': '+'}
    assert expr.children == {'left': Var('a'), 'right': Var('b')}
    assert list(expr.iter_children()) == [Var('a'), Var('b')]


def test_evolve():
    expr = Binop('+', Var('a'), Var('b'))
    actual = evolve(expr, {'left': Var('x')})
    assert actual == Binop('+', Var('x'), Var('b'))


def test_rewrite():
    expr = Binop('+', Var('a'), Binop('+', Var('b'), Var('c')))
    actual = rewrite(expr, lambda t: Var(t.name + '_') if isinstance(t, Var) else None)
    assert actual == Binop('+', Var('a_'), Binop('+', Var('b_'), Var('c_')))
