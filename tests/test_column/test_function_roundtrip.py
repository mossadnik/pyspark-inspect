import pytest
from pyspark_inspect import inspect_column
import pyspark_inspect.column.expression as E
from pyspark_inspect.column.expression import FunctionName as F


a, b, c = map(E.ColumnReference, 'abc')
number = E.Literal(1)
text = E.Literal('a')


@pytest.mark.parametrize('name, args', [
    # operators
    (F.eq, (a, b)),
    (F.not_, (a,)),
    (F.and_, (a, b)),
    (F.or_, (a, b)),
    (F.add, (a, b)),
    (F.mul, (a, b)),
    (F.sub, (a, b)),
    (F.truediv, (a, b)),

    # predicate functions
    (F.isnull, (a,)),
    (F.isnotnull, (a,)),

    # conditional functions
    (F.coalesce, (a, b)),

    # string functions
    (F.concat, (a, b)),
    (F.concat_ws, (text, a, b)),
    (F.length, (a,)),
    (F.lower_, (a,)),
    (F.trim, (a,)),
    (F.upper_, (a,)),

    # mathematical functions
    (F.abs_, (a,)),
    (F.greatest, (a, b)),
    (F.least, (a, b)),
    (F.negative, (a,)),

    # aggregate functions
    (F.avg, (a,)),
    (F.count_, (a,)),
    (F.max_, (a,)),
    (F.min_, (a,)),
    (F.sum_, (a,)),

    # array functions
    (F.size, (a,)),
])
def test_function_roundtrip(name, args, spark_session):
    expr = E.Function(name, args)
    col = expr.to_col()
    actual = inspect_column(col)
    assert actual == expr
