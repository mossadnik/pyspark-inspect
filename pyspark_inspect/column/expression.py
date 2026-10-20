import operator as op
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

import pyspark
from pyspark.sql import Column, Window
from pyspark.sql import functions as F


class FunctionName(StrEnum):
    # operators
    eq = '=='
    not_ = 'not'
    and_ = 'and'
    or_ = 'or'
    add = '+'
    sub = '-'
    mul = '*'
    truediv = '/'

    # predicate functions
    isnotnull = 'isnotnull'
    isnull = 'isnull'

    # conditional functions
    coalesce = 'coalesce'

    # string functions
    concat = 'concat'
    concat_ws = 'concat_ws'
    length = 'length'
    lower_ = 'lower'
    trim = 'trim'
    upper_ = 'upper'

    # mathematical functions
    abs_ = 'abs'
    greatest = 'greatest'
    least = 'least'
    negative = 'negative'

    # aggregate functions
    avg = 'avg'
    collect_list = 'collect_list'
    collect_set = 'collect_set'
    count_ = 'count'
    first = 'first'
    max_ = 'max'
    min_ = 'min'
    sum_ = 'sum'

    # array functions
    size = 'size'


# Enforce python value when running on pyspark 3.X.X
FORCE_VALUE_SPARK_3XX = {
    FunctionName.concat_ws: {0,},
    FunctionName.first: {1,},
}


@dataclass
class Expression:
    @property
    def children(self) -> 'list[Expression]':
        return []

    def to_col(self) -> Column:
        raise NotImplementedError()


@dataclass
class UnknownExpression(Expression):
    source: str
    _children: tuple[Expression, ...]

    @property
    def children(self):
        return list(self._children)


@dataclass
class ColumnReference(Expression):
    name: str

    def to_col(self):
        return F.col(self.name)


@dataclass
class Literal(Expression):
    value: Any

    def to_col(self):
        return F.lit(self.value)


@dataclass
class ColumnAlias(Expression):
    _alias: tuple[str, ...]
    child: Expression

    @property
    def name(self) -> str:
        # TODO what is the meaning of multiple aliases?
        return self._alias[0]

    @property
    def children(self):
        return [self.child]

    def to_col(self):
        return self.child.to_col().alias(self.name)


@dataclass
class SortOrder(Expression):
    asc: bool
    nulls_first: bool
    child: Expression

    @property
    def children(self):
        return [self.child]

    def to_col(self):
        col = self.child.to_col()
        if self.asc:
            if self.nulls_first:
                return col.asc_nulls_first()
            else:
                return col.asc_nulls_last()
        else:
            if self.nulls_first:
                return col.desc_nulls_first()
            else:
                return col.desc_nulls_last()


@dataclass
class Function(Expression):
    name: FunctionName
    args: tuple[Expression, ...]

    @property
    def children(self):
        return list(self.args)

    def to_col(self):
        func = RESOLVED_FUNCTION.get(self.name)
        if not func:
            raise NotImplementedError(f'Function {self.name} is not supported')
        args = []
        if pyspark.__version__.startswith('3'):
            force_value = FORCE_VALUE_SPARK_3XX.get(self.name, set())
        else:
            force_value = set()
        for i, a in enumerate(self.args):
            if i in force_value:
                if not isinstance(a, Literal):
                    raise ValueError(f'Argument {i} of function {self.name} must be a Python value for pyspark < 4.0.0.')
                args.append(a.value)
            else:
                args.append(a.to_col())
        return func(*args)


@dataclass
class CaseWhen(Expression):
    branches: tuple[tuple[Expression, Expression], ...]
    otherwise: Expression | None

    @property
    def children(self):
        res = [c for b in self.branches for c in b]
        if self.otherwise is None:
            return res
        return res + [self.otherwise]

    def to_col(self):
        b = self.branches[0]
        res = F.when(b[0].to_col(), b[1].to_col())
        for b in self.branches[1:]:
            res = res.when(b[0].to_col(), b[1].to_col())
        if self.otherwise is None:
            return res
        return res.otherwise(self.otherwise.to_col())


@dataclass
class WindowExpression(Expression):
    # TODO add frame
    window_function: Expression
    partition_by: tuple[Expression, ...]
    order_by: tuple[Expression, ...]

    @property
    def children(self):
        return [self.window_function, *self.partition_by, *self.order_by]

    def to_col(self):
        window = (
            Window()
            .partitionBy([c.to_col() for c in self.partition_by])
            .orderBy([c.to_col() for c in self.order_by])
        )
        func = self.window_function.to_col()
        return func.over(window)


@dataclass
class UserDefinedFunction(Expression):
    function_name: str
    args: tuple[Expression, ...]

    @property
    def children(self):
        return list(self.args)


RESOLVED_FUNCTION = {
    # operators
    FunctionName.eq: op.eq,
    FunctionName.not_: op.inv,
    FunctionName.and_: op.and_,
    FunctionName.or_: op.or_,
    FunctionName.add: op.add,
    FunctionName.sub: op.sub,
    FunctionName.mul: op.mul,
    FunctionName.truediv: op.truediv,

    # conditional functions
    FunctionName.coalesce: F.coalesce,

    # predicate functions
    FunctionName.isnull: F.isnull,
    FunctionName.isnotnull: F.isnotnull,

    # string functions
    FunctionName.concat: F.concat,
    FunctionName.concat_ws: F.concat_ws,
    FunctionName.length: F.length,
    FunctionName.lower_: F.lower,
    FunctionName.trim: F.trim,
    FunctionName.upper_: F.upper,

    # mathematical functions
    FunctionName.abs_: F.abs,
    FunctionName.greatest: F.greatest,
    FunctionName.least: F.least,
    FunctionName.negative: F.negative,

    # aggregate functions
    FunctionName.avg: F.avg,
    FunctionName.collect_list: F.collect_list,
    FunctionName.collect_set: F.collect_set,
    FunctionName.first: F.first,
    FunctionName.count_: F.count,
    FunctionName.max_: F.max,
    FunctionName.min_: F.min,
    FunctionName.sum_: F.sum,

    # array functions
    FunctionName.size: F.size,
}
