"""Parse Catalyst JSON plan into Python data structures."""

import datetime
import json
import typing as tp
from dataclasses import dataclass

from pyspark.errors import PySparkAttributeError
from pyspark.sql import Column

from pyspark_inspect.column import expression as E
from pyspark_inspect.column.expression import FunctionName as F


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


def parse_catalyst_column(col: Column) -> E.Expression:
    expr = get_column_expression(col)
    plan = load_catalyst_plan(expr)
    return parse_catalyst_expression(plan)


def parse_catalyst_expression(expr: CatalystPlan) -> E.Expression:
    children = [parse_catalyst_expression(c) for c in expr.children]

    if expr.class_name == 'org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute':
        return E.ColumnReference('.'.join(parse_array_string(expr.data['nameParts'])))

    if expr.class_name == 'org.apache.spark.sql.catalyst.expressions.Alias':
        return E.ColumnAlias(
            _alias=(expr.data['name'],),
            child=children[0]
        )

    if expr.class_name == 'org.apache.spark.sql.catalyst.expressions.Literal':
        value = expr.data['value']
        data_type = expr.data['dataType']
        if data_type == 'integer':
            value = int(value)
        elif data_type == 'boolean':
            value = value == 'true'
        elif data_type == 'date':
            value = datetime.date.fromisoformat(value)
        elif data_type == 'timestamp':
            value = datetime.datetime.fromisoformat(value)
        return E.Literal(value)

    if expr.class_name in RESOLVED_FUNCTION:
        name = RESOLVED_FUNCTION[expr.class_name]
        if name == F.first:
            ignore_nulls = expr.data.get('ignoreNulls')
            if ignore_nulls is not None and len(children) == 1:
                args = (children[0], E.Literal(ignore_nulls))
        elif name == F.regexp_replace:
            args = tuple(children[:3])
        else:
            args = tuple(children)

        return E.Function(name, args=args)

    if expr.class_name == 'org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression':
        return parse_catalyst_expression(expr.children[0])

    if expr.class_name == 'org.apache.spark.sql.catalyst.expressions.CaseWhen':
        branches = [
            (
                parse_catalyst_expression(load_catalyst_plan(b['_1'])),
                parse_catalyst_expression(load_catalyst_plan(b['_2'])),)
            for b in expr.data['branches']
        ]
        otherwise = expr.data.get('elseValue')
        if otherwise is not None:
            otherwise = parse_catalyst_expression(load_catalyst_plan(otherwise))
        return E.CaseWhen(branches=tuple(branches), otherwise=otherwise)

    if expr.class_name == 'org.apache.spark.sql.catalyst.analysis.UnresolvedFunction':
        name = UNRESOLVED_FUNCTION.get(expr.data['nameParts'])
        if name == F.count_ and expr.data.get('isDistinct'):
            name = F.count_distinct
        if name is not None:
            return E.Function(name=name, args=tuple(children))

    if expr.class_name == 'org.apache.spark.sql.catalyst.analysis.UnresolvedExtractValue':
        child = children[expr.data['child']]
        key = children[expr.data['extraction']]
        return E.GetItem(child=child, key=key)

    if expr.class_name == 'org.apache.spark.sql.catalyst.expressions.GeneratorOuter':
        child = children[0]
        if isinstance(child, E.Function) and child.name == F.explode:
            return E.Function(name=F.explode_outer, args=child.args)

    return E.UnknownExpression(expr.class_name, tuple(children))


def parse_array_string(s: str) -> tuple[str, ...]:
    """split comma separated array.

    Note that not all values can be parsed reliably since values are not quoted.
    """
    stripped = s[1:-1]
    if not stripped:
        return ()
    return tuple(stripped.split(', '))


RESOLVED_FUNCTION = {
    # predicate functions
    'org.apache.spark.sql.catalyst.expressions.IsNotNull': F.isnotnull,
    'org.apache.spark.sql.catalyst.expressions.IsNull': F.isnull,

    # operators
    'org.apache.spark.sql.catalyst.expressions.Add': F.add,
    'org.apache.spark.sql.catalyst.expressions.And': F.and_,
    'org.apache.spark.sql.catalyst.expressions.Divide': F.truediv,
    'org.apache.spark.sql.catalyst.expressions.EqualTo': F.eq,
    'org.apache.spark.sql.catalyst.expressions.Not': F.not_,
    'org.apache.spark.sql.catalyst.expressions.Or': F.or_,
    'org.apache.spark.sql.catalyst.expressions.Multiply': F.mul,
    'org.apache.spark.sql.catalyst.expressions.Subtract': F.sub,
    'org.apache.spark.sql.catalyst.expressions.UnaryMinus': F.negative,

    # conditional functions
    'org.apache.spark.sql.catalyst.expressions.Coalesce': F.coalesce,

    # string functions
    'org.apache.spark.sql.catalyst.expressions.Concat': F.concat,
    'org.apache.spark.sql.catalyst.expressions.ConcatWs': F.concat_ws,
    'org.apache.spark.sql.catalyst.expressions.FormatString': F.format_string,
    'org.apache.spark.sql.catalyst.expressions.Length': F.length,
    'org.apache.spark.sql.catalyst.expressions.Lower': F.lower_,
    'org.apache.spark.sql.catalyst.expressions.StringLPad': F.lpad,
    'org.apache.spark.sql.catalyst.expressions.RegExpExtract': F.regexp_extract,
    'org.apache.spark.sql.catalyst.expressions.RegExpReplace': F.regexp_replace,
    'org.apache.spark.sql.catalyst.expressions.StringRPad': F.rpad,
    'org.apache.spark.sql.catalyst.expressions.StringSplit': F.split_,
    'org.apache.spark.sql.catalyst.expressions.Substring': F.substring,
    'org.apache.spark.sql.catalyst.expressions.SubstringIndex': F.substring_index,
    'org.apache.spark.sql.catalyst.expressions.StringTrim': F.trim,
    'org.apache.spark.sql.catalyst.expressions.Upper': F.upper_,

    # datetime functions
    'org.apache.spark.sql.catalyst.expressions.CurrentDate': F.current_date,
    'org.apache.spark.sql.catalyst.expressions.CurrentTimestamp': F.current_timestamp,
    'org.apache.spark.sql.catalyst.expressions.DateDiff': F.datediff,
    'org.apache.spark.sql.catalyst.expressions.DateFormatClass': F.date_format,
    'org.apache.spark.sql.catalyst.expressions.FromUnixTime': F.from_unixtime,
    'org.apache.spark.sql.catalyst.expressions.Month': F.month,
    'org.apache.spark.sql.catalyst.expressions.ParseToDate': F.to_date,
    'org.apache.spark.sql.catalyst.expressions.UnixMillis': F.unix_millis,
    'org.apache.spark.sql.catalyst.expressions.Year': F.year,

    # mathematical functions
    'org.apache.spark.sql.catalyst.expressions.Abs': F.abs_,
    'org.apache.spark.sql.catalyst.expressions.Hex': F.hex,
    'org.apache.spark.sql.catalyst.expressions.Least': F.least,
    'org.apache.spark.sql.catalyst.expressions.Greatest': F.greatest,
    'org.apache.spark.sql.catalyst.expressions.Round': F.round,

    # hash functions
    'org.apache.spark.sql.catalyst.expressions.Md5': F.md5,
    'org.apache.spark.sql.catalyst.expressions.XxHash64': F.xxhash64,

    # array functions
    'org.apache.spark.sql.catalyst.expressions.CreateArray': F.array,
    'org.apache.spark.sql.catalyst.expressions.ArrayDistinct': F.array_distinct,
    'org.apache.spark.sql.catalyst.expressions.Flatten': F.flatten,
    'org.apache.spark.sql.catalyst.expressions.Size': F.size,
    'org.apache.spark.sql.catalyst.expressions.SortArray': F.sort_array,

    # struct functions
    'org.apache.spark.sql.catalyst.expressions.CreateNamedStruct': F.named_struct,

    # aggregate functions
    'org.apache.spark.sql.catalyst.expressions.aggregate.Average': F.avg,
    'org.apache.spark.sql.catalyst.expressions.aggregate.Count': F.count_,
    'org.apache.spark.sql.catalyst.expressions.aggregate.CollectList': F.collect_list,
    'org.apache.spark.sql.catalyst.expressions.aggregate.CollectSet': F.collect_set,
    'org.apache.spark.sql.catalyst.expressions.aggregate.First': F.first,
    'org.apache.spark.sql.catalyst.expressions.aggregate.Max': F.max_,
    'org.apache.spark.sql.catalyst.expressions.aggregate.Min': F.min_,
    'org.apache.spark.sql.catalyst.expressions.Rank': F.rank,
    'org.apache.spark.sql.catalyst.expressions.aggregate.Sum': F.sum_,

    # window functions
    'org.apache.spark.sql.catalyst.expressions.Lag': F.lag,
    'org.apache.spark.sql.catalyst.expressions.Lead': F.lead,
    'org.apache.spark.sql.catalyst.expressions.RowNumber': F.row_number,

    # generator functions
    'org.apache.spark.sql.catalyst.expressions.Explode': F.explode,
}


# Catalyst org.apache.spark.sql.catalyst.analysis.UnresolvedFunction
# Seen on Databricks 3.5.2
UNRESOLVED_FUNCTION = {
    # predicate functions
    '[isnotnull]': F.isnotnull,
    '[isNotNull]': F.isnotnull,
    '[isNull]': F.isnull,
    '[isnull]': F.isnull,

    # operators
    '[+]': F.add,
    '[and]': F.and_,
    '[=]': F.eq,
    '[*]': F.mul,
    '[!]': F.not_,
    '[or]': F.or_,
    '[-]': F.sub,
    '[/]': F.truediv,

    # conditional functions
    '[coalesce]': F.coalesce,

    # string functions
    '[concat]': F.concat,
    '[concat_ws]': F.concat_ws,
    '[length]': F.length,
    '[lower]': F.lower_,
    '[trim]': F.trim,
    '[upper]': F.upper_,

    # mathematical functions
    '[abs]': F.abs_,
    '[greatest]': F.greatest,
    '[least]': F.least,
    '[negative]': F.negative,

    # aggregate functions
    '[avg]': F.avg,
    '[count]': F.count_,
    '[collect_list]': F.collect_list,
    '[collect_set]': F.collect_set,
    '[first]': F.first,
    '[max]': F.max_,
    '[min]': F.min_,
    '[sum]': F.sum_,

    # array functions
    '[size]': F.size,
}
