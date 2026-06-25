"""Parse Catalyst JSON plan into Python data structures."""

import typing as tp
import json
from dataclasses import dataclass
from pyspark.errors import PySparkAttributeError
from pyspark.sql import DataFrame, Column, types as T
from pyspark_inspect import plan as P, expression as E


@dataclass
class CatalystPlan:
    children: list['CatalystPlan']
    data: dict[str, tp.Any]

    @property
    def class_name(self) -> str:
        return self.data['class']


def add_node(stack, plan: CatalystPlan, num_children: int):
    if num_children > 0 or not stack:
        stack.append((plan, num_children))
    else:
        top, num_children = stack[-1]
        top.children.append(plan)
        if len(top.children) == num_children and len(stack) > 0:
            stack.pop()
            add_node(stack, top, 0)


def get_dataframe_plan(df: DataFrame) -> str:
    """Get Catalyst json representation of a DataFrame query plan."""
    try:
        jdf = df._jdf
    except PySparkAttributeError:
        raise NotImplementedError('Cannot retrieve query plan from JVM. Are you using Spark Connect?')
    return jdf.logicalPlan().toJSON()


def get_column_expression(c: Column) -> str:
    """Get Catalyst json representation of a column expression."""
    try:
        jc = c._jc
    except PySparkAttributeError:
        raise NotImplementedError('Cannot retrieve query plan from JVM. Are you using Spark Connect?')
    return jc.expr().toJSON()


def load_catalyst_plan(plan: str | list) -> CatalystPlan:
    """Load Catalyst plan string or parsed JSON into raw parse tree."""
    if isinstance(plan, str):
        plan = tp.cast(list, json.loads(plan))
    stack: list[tuple[CatalystPlan, int]] = []
    for item in plan:
        num_children = item['num-children']
        node = CatalystPlan(
            children=[],
            data=item
        )
        add_node(stack, node, num_children)
    assert len(stack) == 1
    return stack[0][0]


def _parse_logical_relation(plan: CatalystPlan, children: list[P.Plan]) -> P.Table:
    tid = plan.data['catalogTable']['identifier']
    qualified_name = '.'.join([tid['catalog'], tid['database'], tid['table']])
    columns = [parse_expression(load_catalyst_plan(c)) for c in plan.data['output']]
    return P.Table(qualified_name=qualified_name, columns=tuple(columns))


def _parse_subquery_alias(plan: CatalystPlan, children: list[P.Plan]) -> P.Alias:
    return P.Alias(
        child=children[0],
        alias=plan.data['identifier']['name'],
        qualifier=parse_array_string(plan.data['identifier'].get('qualifier', ''))
    )

def _parse_project(plan: CatalystPlan, children: list[P.Plan]) -> P.Project:
    return P.Project(
        child=children[0],
        columns=tuple([parse_expression(load_catalyst_plan(p)) for p in plan.data['projectList']])
    )


def _parse_window(plan: CatalystPlan, children: list[P.Plan]) -> P.Project:
    child = tp.cast(P.Project, children[0])
    columns = tuple([
        *child.columns,
        *[parse_expression(load_catalyst_plan(p)) for p in plan.data['windowExpressions']],
    ])
    return P.Project(child=child.child, columns=columns)


def _parse_join(plan: CatalystPlan, children: list[P.Plan]) -> P.Join:
    how = JOIN_TYPE[plan.data['joinType']['object']]
    on = parse_expression(load_catalyst_plan(plan.data['condition']))
    return P.Join(left=children[0], right=children[1], on=on, how=how)


def _parse_one_row_relation(plan: CatalystPlan, children: list[P.Plan]) -> P.OneRowRelation:
    return P.OneRowRelation()


def _parse_cte_relation_def(plan: CatalystPlan, children: list[P.Plan]) -> P.Plan:
    return P.CTEDef(
        cte_id=plan.data['id'],
        child=children[0]
    )


def _parse_cte_relation_ref(plan: CatalystPlan, children: list[P.Plan]) -> P.Plan:
    return P.CTERef(cte_id=plan.data['cteId'])


def _parse_with_cte(plan: CatalystPlan, children: list[P.Plan]) -> P.Plan:
    return P.WithCTE(
        ctes=tuple(children[:-1]),
        main=children[-1]
    )


def _parse_union(plan: CatalystPlan, children: list[P.Plan]) -> P.Plan:
    return P.Union(
        left=children[0],
        right=children[1],
        by_name=plan.data['byName'],
        allow_missing_columns=plan.data['allowMissingCol']
    )


def _parse_except(plan: CatalystPlan, children: list[P.Plan]) -> P.Plan:
    return P.Except(
        left=children[0],
        right=children[1],
        is_all=plan.data['isAll'],
    )


def _parse_intersect(plan: CatalystPlan, children: list[P.Plan]) -> P.Plan:
    return P.Intersect(
        left=children[0],
        right=children[1],
        is_all=plan.data['isAll'],
    )

def _parse_local_limit(plan: CatalystPlan, children: list[P.Plan]) -> P.Plan:
    limit = int(plan.data['limitExpr'][0]['value'])
    return P.LocalLimit(children[0], limit=limit)


def _parse_global_limit(plan: CatalystPlan, children: list[P.Plan]) -> P.Plan:
    limit = int(plan.data['limitExpr'][0]['value'])
    return P.GlobalLimit(children[0], limit=limit)


def _parse_aggregate(plan: CatalystPlan, children: list[P.Plan]) -> P.Plan:
    return P.Aggregate(
        child=children[0],
        grouping_expressions=tuple([parse_expression(load_catalyst_plan(p)) for p in plan.data['groupingExpressions']]),
        columns=tuple([parse_expression(load_catalyst_plan(p)) for p in plan.data['aggregateExpressions']])
    )


def _parse_distinct(plan: CatalystPlan, children: list[P.Plan]) -> P.Plan:
    return P.Distinct(children[0])


def _skip_unary(plan: CatalystPlan, children: list[P.Plan]) -> P.Plan:
    """Skip a unary plan node, e.g. repartition"""
    return children[0]


LOGICAL_PLAN = 'org.apache.spark.sql.catalyst.plans.logical'

JOIN_TYPE = {
    'org.apache.spark.sql.catalyst.plans.Inner$': 'inner',
    'org.apache.spark.sql.catalyst.plans.LeftOuter$': 'left',
    'org.apache.spark.sql.catalyst.plans.RightOuter$': 'right',
    'org.apache.spark.sql.catalyst.plans.FullOuter$': 'outer',
    'org.apache.spark.sql.catalyst.plans.LeftSemi$': 'left-semi',
    'org.apache.spark.sql.catalyst.plans.LeftAnti$': 'left-anti',
}

PLAN_PARSER: dict[str, tp.Callable[[CatalystPlan, list[P.Plan]], P.Plan]] = {
    'org.apache.spark.sql.execution.datasources.LogicalRelation': _parse_logical_relation,
    f'{LOGICAL_PLAN}.Aggregate': _parse_aggregate,
    f'{LOGICAL_PLAN}.CTERelationDef': _parse_cte_relation_def,
    f'{LOGICAL_PLAN}.CTERelationRef': _parse_cte_relation_ref,
    f'{LOGICAL_PLAN}.Distinct': _parse_distinct,
    f'{LOGICAL_PLAN}.Except': _parse_except,
    f'{LOGICAL_PLAN}.GlobalLimit': _parse_global_limit,
    f'{LOGICAL_PLAN}.Intersect': _parse_intersect,
    f'{LOGICAL_PLAN}.Join': _parse_join,
    f'{LOGICAL_PLAN}.LocalLimit': _parse_local_limit,
    f'{LOGICAL_PLAN}.SubqueryAlias': _parse_subquery_alias,
    f'{LOGICAL_PLAN}.OneRowRelation': _parse_one_row_relation,
    f'{LOGICAL_PLAN}.Project': _parse_project,
    f'{LOGICAL_PLAN}.Union': _parse_union,
    f'{LOGICAL_PLAN}.Window': _parse_window,
    f'{LOGICAL_PLAN}.WithCTE': _parse_with_cte,
    # skipped
    f'{LOGICAL_PLAN}.Repartition': _skip_unary,
    f'{LOGICAL_PLAN}.RepartitionByExpression': _skip_unary,
    f'{LOGICAL_PLAN}.Sort': _skip_unary,
}


def parse_plan(plan: CatalystPlan):
    children = [parse_plan(c) for c in plan.children]
    parser = PLAN_PARSER.get(plan.class_name)
    if not parser:
        raise ValueError(f'Unsupported plan: {plan.class_name}')
    return parser(plan, children)


def _parse_expression_alias(plan: CatalystPlan, children: list[E.Expression]) -> E.Expression:
    return E.Alias(
        arg=children[0],
        name=plan.data['name'],
        qualifier=parse_array_string(plan.data['qualifier'])
    )


def _parse_attribute_reference(plan: CatalystPlan, children: list[E.Expression]) -> E.Expression:
    # TODO consider to parse data type: T._parse_datatype_json_value(plan.data['dataType'])
    return E.AttributeReference(
        name=plan.data['name'],
        qualifier=parse_array_string(plan.data['qualifier']),
        attribute_id=plan.data['exprId']['id']
    )


def _parse_literal(plan: CatalystPlan, children: list[E.Expression]) -> E.Expression:
    # TODO: Should cast values
    raw_value = plan.data['value']
    raw_data_type = plan.data['dataType']
    return E.Literal(value=raw_value, data_type=raw_data_type)


def _parse_variadic(cls: type) -> tp.Callable:
    """Generic parser vor variadic expressions without additional arguments."""
    def parser(plan: CatalystPlan, children: list[E.Expression]):
        return cls(tuple(children))
    return parser


def _parse_binary(cls: type) -> tp.Callable:
    """Generic parser for binary expressions without additional arguments."""
    def parser(plan: CatalystPlan, children: list[E.Expression]):
        return cls(children[0], children[1])
    return parser


def _parse_unary(cls: type) -> tp.Callable:
    """Generic parser for unary expressions without additional arguments."""
    def parser(plan: CatalystPlan, children: list[E.Expression]):
        return cls(children[0])
    return parser


EXPRESSION_PARSER: dict[str, tp.Callable[[CatalystPlan, list[E.Expression]], E.Expression]] = {
    'org.apache.spark.sql.catalyst.expressions.Alias': _parse_expression_alias,
    'org.apache.spark.sql.catalyst.expressions.Coalesce': _parse_variadic(E.Coalesce),
    'org.apache.spark.sql.catalyst.expressions.AttributeReference': _parse_attribute_reference,
    'org.apache.spark.sql.catalyst.expressions.And': _parse_binary(E.And),
    'org.apache.spark.sql.catalyst.expressions.Or': _parse_binary(E.Or),
    'org.apache.spark.sql.catalyst.expressions.EqualTo': _parse_binary(E.Equals),
    'org.apache.spark.sql.catalyst.expressions.EqualNullSafe': _parse_binary(E.EqNullSafe),
    'org.apache.spark.sql.catalyst.expressions.IsNull': _parse_unary(E.IsNull),
    'org.apache.spark.sql.catalyst.expressions.IsNotNull': _parse_unary(E.IsNotNull),
    'org.apache.spark.sql.catalyst.expressions.Literal': _parse_literal,
}


def _parse_unknown_expression(plan: CatalystPlan, children: list[E.Expression]) -> E.Expression:
    """Fallback parser to capture unknown expressions with their children."""
    return E.UnknownExpression(args=tuple(children), class_name=plan.class_name)


def parse_expression(plan: CatalystPlan) -> E.Expression:
    """Parse a column expression."""
    children = [parse_expression(c) for c in plan.children]
    parser = EXPRESSION_PARSER.get(plan.class_name, _parse_unknown_expression)
    return parser(plan, children)


def parse_array_string(s: str) -> tuple[str, ...]:
    """split comma separated array.

    Note that not all values can be parsed reliably since values are not quoted.
    """
    stripped = s[1:-1]
    if not stripped:
        return ()
    return tuple(stripped.split(', '))
