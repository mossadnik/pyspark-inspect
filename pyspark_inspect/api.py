"""High-level public functions and classes."""
from py4j.protocol import Py4JError
from pyspark.sql import Column

from .column.catalyst import parse_catalyst_column
from .column.expression import Expression

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
