import pyspark.sql.connect.expressions as cex
from pyspark.sql import Column

from pyspark_inspect.column.expression import *


def parse_connect_expression(expr: cex.Expression) -> Expression:
    if isinstance(expr, cex.ColumnReference):
        return ColumnReference(name=expr.name())
    elif isinstance(expr, cex.LiteralExpression):
        return Literal(value=expr._value)
    elif isinstance(expr, cex.ColumnAlias):
        # TODO what does it mean to have multiple aliases?
        return ColumnAlias(
            _alias=tuple(expr._alias),
            child=parse_connect_expression(expr._child)
        )
    elif isinstance(expr, cex.UnresolvedFunction):
        name = FUNCTION_SYNONYM.get(expr._name, expr._name)
        args = [parse_connect_expression(arg) for arg in expr._args]
        return Function(
            name=name,
            args=tuple(args)
        )
    elif isinstance(expr, cex.CaseWhen):
        branches = [
            (parse_connect_expression(a), parse_connect_expression(b))
            for a, b in expr._branches
        ]
        otherwise = None
        if expr._else_value is not None:
            otherwise = parse_connect_expression(expr._else_value)
        return CaseWhen(
            branches=tuple(branches),
            otherwise=otherwise
        )
    elif isinstance(expr, cex.WindowExpression):
        return WindowExpression(
            window_function=parse_connect_expression(expr._windowFunction),
            partition_by=tuple([parse_connect_expression(c) for c in expr._windowSpec._partitionSpec]),
            order_by=tuple([parse_connect_expression(c) for c in expr._windowSpec._orderSpec])
        )
    elif isinstance(expr, cex.SortOrder):
        return SortOrder(
            asc=expr._ascending,
            nulls_first=expr._nullsFirst,
            child=parse_connect_expression(expr._child)
        )
    else:
        children = [parse_connect_expression(c) for c in expr.children]
        return UnknownExpression(source=expr, _children=tuple(children))


def parse_connect_column(col: Column) -> Expression:
    """Parse Spark Connect Column into Spark version agnostic AST."""
    return parse_connect_expression(col._expr)


FUNCTION_SYNONYM = {
    'isNull': 'isnull',
    'isNotNull': 'isnotnull',
}
