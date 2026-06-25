from pyspark_inspect import expression as E


def test_variadic_children():
    expr = E.Variadic(args=(E.Literal('a'), E.Literal('b')))
    assert len(expr.children) == 2
    assert all(isinstance(c, E.Expression) for c in expr.children)
