import pytest

import pyspark_inspect.column.expression as E
from pyspark_inspect import inspect_column
from pyspark_inspect.column.expression import FunctionName as F

from .roundtrip_data import function_test_cases

a, b, c = map(E.ColumnReference, 'abc')
number = E.Literal(1)
text = E.Literal('a')
boolean = E.Literal(True)


@pytest.mark.parametrize('name, args', function_test_cases)
def test_function_roundtrip(name, args, spark_session):
    expr = E.Function(name, args)
    col = expr.to_col()
    actual = inspect_column(col)
    assert actual == expr


def test_GetItem_roundtrip(spark_session):
    expr = E.GetItem(child=a, key=b)
    col = expr.to_col()
    actual = inspect_column(col)
    assert actual == expr
