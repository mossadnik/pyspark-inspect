from pyspark_inspect import replace_column_references, get_column_references
from pyspark_inspect.column import expression as E


def test_replace_column_references():
    expr = E.Function(
        name=E.FunctionName.add,
        args=(E.ColumnReference('a'), E.ColumnReference('b'))
    )
    actual = replace_column_references(expr, {'a': E.Literal(1)})
    assert actual == E.Function(
        name=E.FunctionName.add,
        args=(E.Literal(1), E.ColumnReference('b')),
    )


def test_get_column_references():
    expr = E.Function(
        name=E.FunctionName.add,
        args=(E.Literal(1), E.ColumnReference('a'))
    )
    actual = get_column_references(expr)
    assert actual == {'a',}
