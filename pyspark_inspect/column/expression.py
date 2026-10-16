import operator as op
from dataclasses import dataclass
from typing import Any

from pyspark.sql import Column
from pyspark.sql import functions as F, Window


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
    _alias: tuple[str]
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
    name: str
    args: tuple[Expression, ...]

    @property
    def children(self):
        return list(self.args)

    def to_col(self):
        func = RESOLVED_FUNCTION.get(self.name)
        if not func:
            raise NotImplementedError(f'Function {self.name} is not supported')
        return func(*[a.to_col() for a in self.args])

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
    partition_by: tuple[Expression]
    order_by: tuple[Expression]

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
    args: tuple[Expression]

    @property
    def children(self):
        return list(self.args)


RESOLVED_FUNCTION = {
    # operators
    '==': op.eq,
    'not': op.inv,
    'and': op.and_,
    'or': op.or_,
    '+': op.add,
    '-': op.sub,
    '*': op.mul,
    '/': op.truediv,
    # conditional functions
    'coalesce': F.coalesce,
    # predicate functions
    'isnull': F.isnull,
    'isnotnull': F.isnotnull,
    # string functions
    'concat': F.concat,
    'concat_ws': F.concat_ws,
    'length': F.length,
    # aggregate functions
    'avg': F.avg,
    'count': F.count,
    'max': F.max,
    'min': F.min,
    'sum': F.sum,
    # array functions
    'size': F.size,
}
