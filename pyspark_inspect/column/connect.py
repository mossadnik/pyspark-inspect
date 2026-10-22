import datetime
from typing import cast

import pyspark.sql.connect.expressions as cex
from pyspark.sql import Column
from pyspark.sql import types as T

import pyspark_inspect.column.expression as E
from pyspark_inspect.column.expression import FunctionName as F


def parse_connect_expression(expr: cex.Expression) -> E.Expression:
    if isinstance(expr, cex.ColumnReference):
        return E.ColumnReference(name=expr.name())
    if isinstance(expr, cex.LiteralExpression):
        data_type = expr._dataType
        value = expr._value
        if data_type == T.DateType():
            value = datetime.date(1970, 1, 1) + datetime.timedelta(days=cast(int, value))
        elif data_type == T.TimestampType():
            value = datetime.datetime.fromtimestamp(cast(int, value) / 1e6)
        return E.Literal(value=value)
    if isinstance(expr, cex.ColumnAlias):
        # TODO what does it mean to have multiple aliases?
        return E.ColumnAlias(
            _alias=tuple(expr._alias),
            child=parse_connect_expression(expr._child)
        )
    if isinstance(expr, cex.UnresolvedExtractValue):
        return E.GetItem(
            child=parse_connect_expression(expr._child),
            key=parse_connect_expression(expr._extraction)
        )
    if isinstance(expr, cex.UnresolvedFunction):
        name = FUNCTION_SYNONYM.get(expr._name, expr._name)
        args = [parse_connect_expression(arg) for arg in expr._args]
        if name == F.count_ and expr._is_distinct:
            name = F.count_distinct
        if name in E.FunctionName:
            return E.Function(
                name=E.FunctionName(name),
                args=tuple(args)
            )
        else:
            return E.UnknownExpression(expr._name, _children=tuple([parse_connect_expression(a) for a in expr._args]))
    if isinstance(expr, cex.CaseWhen):
        branches = [
            (parse_connect_expression(a), parse_connect_expression(b))
            for a, b in expr._branches
        ]
        otherwise = None
        if expr._else_value is not None:
            otherwise = parse_connect_expression(expr._else_value)
        return E.CaseWhen(
            branches=tuple(branches),
            otherwise=otherwise
        )
    if isinstance(expr, cex.WindowExpression):
        return E.WindowExpression(
            window_function=parse_connect_expression(expr._windowFunction),
            partition_by=tuple([parse_connect_expression(c) for c in expr._windowSpec._partitionSpec]),
            order_by=tuple([parse_connect_expression(c) for c in expr._windowSpec._orderSpec])
        )
    if isinstance(expr, cex.SortOrder):
        return E.SortOrder(
            asc=expr._ascending,
            nulls_first=expr._nullsFirst,
            child=parse_connect_expression(expr._child)
        )

    children = [parse_connect_expression(c) for c in expr.children]
    return E.UnknownExpression(source=str(expr), _children=tuple(children))


def parse_connect_column(col: Column) -> E.Expression:
    """Parse Spark Connect Column into Spark version agnostic AST."""
    return parse_connect_expression(col._expr)


FUNCTION_SYNONYM = {
    'isNull': F.isnull,
    'isNotNull': F.isnotnull,
}
