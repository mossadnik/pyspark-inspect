import operator as op
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

import pyspark
from pyspark.sql import Column, Window
from pyspark.sql import functions as F

from pyspark_inspect import term


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


@dataclass(frozen=True)
class Expression(term.Term):
    def to_col(self) -> Column:
        raise NotImplementedError()


@dataclass(frozen=True)
class UnknownExpression(Expression):
    source: str = term.param()
    _children: tuple[Expression, ...] = term.child()


@dataclass(frozen=True)
class ColumnReference(Expression):
    name: str = term.param()

    def to_col(self):
        return F.col(self.name)


@dataclass(frozen=True)
class Literal(Expression):
    value: Any = term.param()

    def to_col(self):
        return F.lit(self.value)


@dataclass(frozen=True)
class ColumnAlias(Expression):
    _alias: tuple[str, ...] = term.param()
    child: Expression = term.child()

    @property
    def name(self) -> str:
        # TODO what is the meaning of multiple aliases?
        return self._alias[0]

    def to_col(self):
        return self.child.to_col().alias(self.name)


@dataclass(frozen=True)
class GetItem(Expression):
    """Apply __getitem__ to Column."""
    child: Expression = term.child()
    key: Expression = term.child()

    def to_col(self):
        return self.child.to_col()[self.key.to_col()]


@dataclass(frozen=True)
class SortOrder(Expression):
    child: Expression = term.child()
    asc: bool = term.param()
    nulls_first: bool = term.param()

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


@dataclass(frozen=True)
class Function(Expression):
    name: FunctionName = term.param()
    args: tuple[Expression, ...] = term.child()

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


@dataclass(frozen=True)
class CaseWhen(Expression):
    args: tuple[Expression, ...] = term.child()

    def to_col(self):
        args = [a.to_col() for a in self.args]
        branches = [
            (when, then)
            for when, then in zip(args[:2:], args[1:2:])
        ]
        res = F.when(*branches[0])
        for when, then in branches[1:]:
            res = res.when(when, then)
        if len(args) % 2 == 1:
            res = res.otherwise(args[-1])
        return res


@dataclass(frozen=True)
class WindowSpec(Expression):
    partition_by: tuple[Expression, ...] = term.child()
    order_by: tuple[Expression, ...] = term.child()

    def to_col(self):
        raise NotImplementedError('WindowSpec cannot be converted to Column.')


@dataclass(frozen=True)
class WindowExpression(Expression):
    # TODO add frame
    window_function: Expression = term.child()
    window_spec: WindowSpec = term.child()

    def to_col(self):
        window = (
            Window()
            .partitionBy([c.to_col() for c in self.window_spec.partition_by])
            .orderBy([c.to_col() for c in self.window_spec.order_by])
        )
        func = self.window_function.to_col()
        return func.over(window)


@dataclass(frozen=True)
class UserDefinedFunction(Expression):
    """Python UDFs.

    This can not be used to actually run the function as of now.
    """
    function_name: str = term.param()
    args: tuple[Expression, ...] = term.child()


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
