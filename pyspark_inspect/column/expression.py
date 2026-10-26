import operator as op
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

import pyspark
from pyspark.sql import Column, Window
from pyspark.sql import functions as F


class FunctionName(StrEnum):
    # operators
    add = '+'
    and_ = 'and'
    eq = '=='
    mul = '*'
    not_ = 'not'
    or_ = 'or'
    sub = '-'
    truediv = '/'

    # predicate functions
    isnotnull = 'isnotnull'
    isnull = 'isnull'

    # conditional functions
    coalesce = 'coalesce'

    # string functions
    concat = 'concat'
    concat_ws = 'concat_ws'
    encode_ = 'encode'
    endswith_ = 'endswith'
    decode_ = 'decode'
    format_string = 'format_string'
    length = 'length'
    lower_ = 'lower'
    lpad = 'lpad'
    regexp_extract = 'regexp_extract'
    regexp_replace = 'regexp_replace'
    replace_ = 'replace'
    rpad = 'rpad'
    split_ = 'split'
    startswith_ = 'startswith'
    substring = 'substring'
    substring_index = 'substring_index'
    rlike = 'rlike'
    trim = 'trim'
    upper_ = 'upper'

    # datetime functions
    current_date = 'current_date'
    current_timestamp = 'current_timestamp'
    date_add = 'date_add'
    date_format = 'date_format'
    datediff = 'datediff'
    from_unixtime = 'from_unixtime'
    month = 'month'
    timestamp_millis = 'timestamp_millis'
    to_date = 'to_date'
    unix_millis = 'unix_millis'
    year = 'year'

    # mathematical functions
    abs_ = 'abs'
    greatest = 'greatest'
    hex = 'hex'
    least = 'least'
    negative = 'negative'
    round = 'round'

    # hash functions
    md5 = 'md5'
    xxhash64 = 'xxhash64'

    # aggregate functions
    avg = 'avg'
    collect_list = 'collect_list'
    collect_set = 'collect_set'
    count_ = 'count'
    count_distinct = 'count_distinct'
    first = 'first'
    max_ = 'max'
    max_by = 'max_by'
    min_ = 'min'
    sum_ = 'sum'

    # collection functions
    element_at = 'element_at'

    # array functions
    array = 'array'
    array_compact = 'array_compact'
    array_contains = 'array_contains'
    array_distinct = 'array_distinct'
    array_intersect = 'array_intersect'
    array_union = 'array_union'
    flatten = 'flatten'
    size = 'size'
    sort_array = 'sort_array'

    # struct functions
    named_struct = 'named_struct'

    # window functions
    lag = 'lag'
    lead = 'lead'
    rank = 'rank'
    row_number = 'row_number'

    # generator functions
    explode = 'explode'
    explode_outer = 'explode_outer'


# Enforce python value when running on pyspark 3.X.X
FORCE_VALUE_SPARK_3XX = {
    FunctionName.concat_ws: {0,},
    FunctionName.date_format: {1,},
    FunctionName.decode_: {1,},
    FunctionName.encode_: {1,},
    FunctionName.first: {1,},
    FunctionName.format_string: {0,},
    FunctionName.from_unixtime: {1,},
    FunctionName.lag: {1, 2},
    FunctionName.lead: {1, 2},
    FunctionName.lpad: {1, 2},
    FunctionName.regexp_extract: {1, 2},
    FunctionName.regexp_replace: {1,},
    FunctionName.round: {1,},
    FunctionName.rpad: {1, 2},
    FunctionName.sort_array: {1,},
    FunctionName.split_: {1, 2},
    FunctionName.substring: {1, 2},
    FunctionName.substring_index: {1, 2},
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
class GetItem(Expression):
    """Apply __getitem__ to Column."""
    child: Expression
    key: Expression

    @property
    def children(self):
        return [self.child, self.key]

    def to_col(self):
        return self.child.to_col()[self.key.to_col()]


@dataclass
class SortOrder(Expression):
    child: Expression
    asc: bool
    nulls_first: bool

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
class ExpressionList(Expression):
    args: tuple[Expression, ...]

    @property
    def children(self):
        return list(self.args)

    def to_col(self) -> list[Column]:
        return [a.to_col() for a in self.args]


@dataclass
class WindowSpec(Expression):
    partition_by: ExpressionList
    order_by: ExpressionList

    @property
    def children(self):
        return [self.partition_by, self.order_by]

    def to_col(self):
        raise NotImplementedError('WindowSpec cannot be converted to Column.')


@dataclass
class WindowExpression(Expression):
    # TODO add frame
    window_function: Expression
    window_spec: WindowSpec

    @property
    def children(self):
        return [self.window_function, self.window_spec]

    def to_col(self):
        window = (
            Window()
            .partitionBy(self.window_spec.partition_by.to_col())
            .orderBy(self.window_spec.order_by.to_col())
        )
        func = self.window_function.to_col()
        return func.over(window)


@dataclass
class UserDefinedFunction(Expression):
    """Python UDFs.

    This can not be used to actually run the function as of now.
    """
    function_name: str
    args: tuple[Expression, ...]

    @property
    def children(self):
        return list(self.args)


def make_function_not_found(name: str) -> Callable:
    """Fallback function if installed pyspark version doesn't have a function that is implemented here."""
    def function_not_found(*args, **kwargs):
        raise NotImplementedError(f'Function {name} can not be found in pyspark.sql.functions.')
    return function_not_found


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
}
for name in FunctionName:
    if name in RESOLVED_FUNCTION:
        continue
    func = getattr(F, name, None)
    if func is None:
        func = make_function_not_found(name)
    RESOLVED_FUNCTION[name] = func
