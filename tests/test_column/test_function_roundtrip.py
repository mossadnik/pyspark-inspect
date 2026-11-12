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


def test_WindowExpression_roundtrip(spark_session):
    expr = E.WindowExpression(
        window_function=E.Function(E.FunctionName.max_, (a,)),
        window_spec=E.WindowSpec(
            partition_by=(b,),
            order_by=(E.SortOrder(child=c, asc=True, nulls_first=True),)
        )
    )
    col = expr.to_col()
    actual = inspect_column(col)
    assert actual == expr


def test_CaseWhen_roundtrip(spark_session):
    expr = E.CaseWhen((
        a, b, c
    ))
    col = expr.to_col()
    actual = inspect_column(col)
    assert actual == expr
