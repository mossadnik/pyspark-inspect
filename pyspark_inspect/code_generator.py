"""Translate plans into pyspark DataFrames and Columns."""

import typing as tp
from functools import singledispatch
from pyspark.sql import SparkSession, DataFrame, Column, functions as F
from . import plan as P
from . import expression as E


@singledispatch
def get_spark_expression(expr: tp.Any) -> Column:
    raise NotImplementedError(f'Cannot translate expression of type {type(expr)}')


@get_spark_expression.register
def _(expr: E.Coalesce) -> Column:
    return F.coalesce(*map(get_spark_expression, expr.children))


@get_spark_expression.register
def _(expr: E.Alias) -> Column:
    return get_spark_expression(expr.arg).alias(expr.name)

@get_spark_expression.register
def _(expr: E.AttributeReference) -> Column:
    return F.col(f'`{".".join(expr.qualifier)}`.`{expr.name}`')


@singledispatch
def get_plan_dataframe(plan: P.Plan, spark: SparkSession) -> DataFrame:
    """convert a plan into a Dataframe."""
    raise NotImplementedError(f'Cannot translate plan of type {type(plan)}')


@get_plan_dataframe.register
def _(plan: P.Project, spark: SparkSession) -> DataFrame:
    df = get_plan_dataframe(plan.child, spark)
    return df.select(*[
        get_spark_expression(expr)
        for expr in plan.columns
    ])


@get_plan_dataframe.register
def _(plan: P.Table, spark: SparkSession) -> DataFrame:
    return spark.read.table(plan.qualified_name)


@get_plan_dataframe.register
def _(plan: P.Alias, spark: SparkSession) -> DataFrame:
    df = get_plan_dataframe(plan.child, spark)
    return df.alias('.'.join([*plan.qualifier, plan.alias]))
