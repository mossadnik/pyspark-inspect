"""Classes representing steps in a query plan."""

import typing as tp
from dataclasses import dataclass
from .expression import Expression
from pyspark.sql import types as T


@dataclass(frozen=True)
class Plan:
    @property
    def children(self) -> list['Plan']:
        return []


@dataclass(frozen=True)
class UnaryPlan(Plan):
    child: Plan

    @property
    def children(self) -> list[Plan]:
        return [self.child]


@dataclass(frozen=True)
class BinaryPlan(Plan):
    left: Plan
    right: Plan

    @property
    def children(self) -> list[Plan]:
        return [self.left, self.right]


@dataclass(frozen=True)
class Alias(UnaryPlan):
    alias: str
    qualifier: tuple[str, ...]


@dataclass(frozen=True)
class Project(UnaryPlan):
    """A projection is a select without row generators like explode."""
    columns: tuple[Expression, ...]


@dataclass(frozen=True)
class Filter(UnaryPlan):
    condition: Expression


@dataclass(frozen=True)
class Join(BinaryPlan):
    """A left/right/inner/outer/cross join."""
    on: Expression
    how: tp.Literal['left', 'right', 'inner', 'outer', 'left-anti', 'left-semi']


@dataclass(frozen=True)
class Table(Plan):
    """A catalog table."""
    qualified_name: str
    columns: tuple[Expression, ...]


@dataclass(frozen=True)
class RDD(Plan):
    """Output of SparkSesssion.createDataFrame.

    Note that we cannot reconstruct data, would need to feed from
    user.
    """
    schema: T.StructType


@dataclass(frozen=True)
class OneRowRelation(Plan):
    """Dummy input for SQL select without from clause."""
    pass


@dataclass(frozen=True)
class WithCTE(Plan):
    """Statement with with-block."""
    ctes: tuple[Plan, ...]
    main: Plan

    @property
    def children(self):
        return [*self.ctes, self.main]


@dataclass(frozen=True)
class CTEDef(UnaryPlan):
    cte_id: int


@dataclass(frozen=True)
class CTERef(Plan):
    cte_id: int


@dataclass(frozen=True)
class Union(BinaryPlan):
    by_name: bool
    allow_missing_columns: bool


@dataclass(frozen=True)
class Except(BinaryPlan):
    is_all: bool


@dataclass(frozen=True)
class Intersect(BinaryPlan):
    is_all: bool


@dataclass(frozen=True)
class Aggregate(UnaryPlan):
    grouping_expressions: tuple[Expression, ...]
    columns: tuple[Expression, ...]


@dataclass(frozen=True)
class Distinct(UnaryPlan):
    pass


@dataclass(frozen=True)
class GlobalLimit(UnaryPlan):
    limit: int


@dataclass(frozen=True)
class LocalLimit(UnaryPlan):
    limit: int
