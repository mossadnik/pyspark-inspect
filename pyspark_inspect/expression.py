"""Representation of Column expressions."""


import typing as tp
import operator as op
from dataclasses import dataclass
from pyspark.sql import Column, functions as F


@dataclass(frozen=True)
class Expression:
    _func: tp.ClassVar[tp.Optional[tp.Callable[..., Column]]] = None

    @property
    def children(self) -> list['Expression']:
        raise NotImplementedError()

    def to_column(self) -> Column:
        raise NotImplementedError()

    def _get_func(self) -> tp.Callable[..., Column]:
        if self._func is None:
            raise NotImplementedError()
        else:
            return self._func


@dataclass(frozen=True)
class Leaf(Expression):
    @property
    def children(self):
        return []


@dataclass(frozen=True)
class UnaryOperator(Expression):
    arg: Expression

    @property
    def children(self):
        return [self.arg]

    def to_column(self) -> Column:
        return self._get_func()(self.arg.to_column())


@dataclass(frozen=True)
class BinaryOperator(Expression):
    left: Expression
    right: Expression

    @property
    def children(self):
        return [self.left, self.right]

    def to_column(self) -> Column:
        return self._get_func()(self.left.to_column(), self.right.to_column())


@dataclass(frozen=True)
class Variadic(Expression):
    args: tuple[Expression, ...]

    @property
    def children(self):
        return list(self.args)

    def to_column(self):
        return self._get_func()(*[arg.to_column() for arg in self.args])


@dataclass(frozen=True)
class UnknownExpression(Variadic):
    class_name: str

    def to_column(self):
        raise NotImplementedError(f'Cannot translate unknown expression of type {self.class_name}')


@dataclass(frozen=True)
class Literal(Leaf):
    value: tp.Hashable
    data_type: str | None = None

    def to_column(self) -> Column:
        return F.lit(self.value)


@dataclass(frozen=True)
class AttributeReference(Leaf):
    name: str
    qualifier: tuple[str, ...]
    attribute_id: str

    @property
    def qualified_name(self) -> str:
        name = f'`{self.name}`'
        if self.qualifier:
            qualifier = '.'.join(self.qualifier)
            return f'`{qualifier}`.{name}'
        else:
            return name

    def to_column(self) -> Column:
        return F.col(self.qualified_name)


@dataclass(frozen=True)
class Alias(UnaryOperator):
    name: str
    qualifier: tuple[str, ...]

    @property
    def qualified_name(self) -> str:
        name = f'`{self.name}`'
        if self.qualifier:
            qualifier = '.'.join(self.qualifier)
            return f'`{qualifier}`.{name}'
        else:
            return name

    def to_column(self) -> Column:
        return self.arg.to_column().alias(self.qualified_name)


@dataclass(frozen=True)
class Equals(BinaryOperator):
    _func = op.eq


@dataclass(frozen=True)
class EqNullSafe(BinaryOperator):
    _func = Column.eqNullSafe


@dataclass(frozen=True)
class GreaterThanOrEqual(BinaryOperator):
    _func = op.ge


@dataclass(frozen=True)
class GreaterThan(BinaryOperator):
    _func = op.gt


@dataclass(frozen=True)
class LessThanOrEqual(BinaryOperator):
    _func = op.le


@dataclass(frozen=True)
class LessThan(BinaryOperator):
    _func = op.lt


@dataclass(frozen=True)
class And(BinaryOperator):
    _func = op.and_


@dataclass(frozen=True)
class Or(BinaryOperator):
    _func = op.or_


@dataclass(frozen=True)
class Not(UnaryOperator):
    _func = op.neg


@dataclass(frozen=True)
class IsNull(UnaryOperator):
    _func = F.isnull


@dataclass(frozen=True)
class IsNotNull(UnaryOperator):
    _func = F.isnotnull


@dataclass(frozen=True)
class Coalesce(Variadic):
    _func = F.coalesce


@dataclass(frozen=True)
class Concat(Variadic):
    _func = F.concat


@dataclass(frozen=True)
class When(Variadic):
    def to_column(self):
        args = [a.to_column() for a in self.args]
        when, then = args[:2]
        c = F.when(when, then)
        for when, then in zip(args[2::2], args[3::2]):
            c = c.when(when, then)
        if len(args) % 2 == 1:
            c = c.otherwise(args[-1])
        return c
