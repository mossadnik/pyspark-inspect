"""High-level public functions and classes."""

from pyspark.sql import DataFrame
from .plan import Plan
from .parser.catalyst import get_dataframe_plan, load_catalyst_plan, parse_plan


def inspect_dataframe(df: DataFrame) -> Plan:
    return parse_plan(load_catalyst_plan(get_dataframe_plan(df)))
