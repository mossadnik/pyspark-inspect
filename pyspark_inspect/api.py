"""High-level public functions and classes."""
from typing import cast

from py4j.protocol import Py4JError
from pyspark.sql import Column

from .column.catalyst import parse_catalyst_column
from .column.expression import ColumnReference, Expression
from .term import Term, rewrite

try:
    from pyspark.sql.connect.column import Column as ConnectColumn

    from .column.connect import parse_connect_column
except (ImportError, ModuleNotFoundError):
    ConnectColumn = None
    parse_connect_column = None

has_connect = ConnectColumn is not None


def inspect_column(col: Column) -> Expression:
    if ConnectColumn is not None and isinstance(col, ConnectColumn):
        return parse_connect_column(col)
    try:
        # >=4.0.0 Raises py4j.Py4JException: Method expr([]) does not exist
        return parse_catalyst_column(col)
    except Py4JError:
        import pyspark
        if pyspark.__version__ >= '4':
            raise NotImplementedError('Only pyspark < 4.0.0 is supported for spark classic')
        else:
            raise


def replace_column_references(expr: Expression, subs: dict[str, Expression]) -> Expression:
    """Replace named column references with expressions."""

    def rule(expr: Term) -> Expression | None:
        if isinstance(expr, ColumnReference):
            return subs.get(expr.name)

    return cast(Expression, rewrite(expr, rule))
