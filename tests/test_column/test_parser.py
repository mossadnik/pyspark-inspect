import pytest
from pyspark.sql import functions as F
from pyspark.sql import types as T

from pyspark_inspect import inspect_column
from pyspark_inspect.column.expression import *


@pytest.fixture
def core_functions(spark_session):
    return [
        {
            'col': F.col('a'),
            'expected': ColumnReference('a'),
            'values': {'a': 1},
        },
        {
            'col': F.lit(1),
            'expected': Literal(1),
            'values': {'a': 1},
        },
        {
            'col': F.col('a').alias('b'),
            'expected': ColumnAlias(_alias=('b',), child=ColumnReference('a'))
        },
    ]




@pytest.fixture
def predicate_functions(spark_session):
    return [
        {
            'col': F.isnotnull('a'),
            'expected': Function(name='isnotnull', args=(ColumnReference('a'),)),
            'values': {'a': 1},
        },
        {
            'col': F.col('a').isNotNull(),
            'expected': Function(name='isnotnull', args=(ColumnReference('a'),)),
            'values': {'a': 1},
        },
        {
            'col': F.isnull('a'),
            'expected': Function(name='isnull', args=(ColumnReference('a'),)),
            'values': {'a': 1},
        },
        {
            'col': F.col('a').isNull(),
            'expected': Function(name='isnull', args=(ColumnReference('a'),)),
            'values': {'a': 1},
        },
    ]


@pytest.fixture
def operators(spark_session):
    a, b = map(F.col, 'ab')
    ref_a, ref_b = map(ColumnReference, 'ab')
    return [
        {
            'col': a == b,
            'expected': Function(name='==', args=(ref_a, ref_b)),
        },
        {
            'col': a != b,
            'expected': Function(
                name='not',
                args=(Function(name='==', args=(ref_a, ref_b)),)
            ),
        },
        {
            'col': a | b,
            'expected': Function(name='or', args=(ref_a, ref_b)),
        },
        {
            'col': a & b,
            'expected': Function(name='and', args=(ref_a, ref_b)),
        },
        {
            'col': a + b,
            'expected': Function(name='+', args=(ref_a, ref_b)),
        },
        {
            'col': a - b,
            'expected': Function(name='-', args=(ref_a, ref_b)),
        },
        {
            'col': a * b,
            'expected': Function(name='*', args=(ref_a, ref_b)),
        },
        {
            'col': a / b,
            'expected': Function(name='/', args=(ref_a, ref_b)),
        },
    ]


@pytest.fixture
def conditional_functions(spark_session):
    a, b = map(F.col, 'ab')
    ref_a, ref_b = map(ColumnReference, 'ab')
    return [
        {
            'col': F.coalesce(a, b),
            'expected': Function(name='coalesce', args=(ref_a, ref_b)),
            'values': {'a': 1, 'b': 2},
        },
        {
            'col': F.when(a, F.lit(1)),
            'expected': CaseWhen(
                branches=((ref_a, Literal(1)),),
                otherwise=None
            ),
        },
        {
            'col': F.when(a, F.lit(1)).otherwise(F.lit(2)),
            'expected': CaseWhen(
                branches=((ref_a, Literal(1)),),
                otherwise=Literal(2)
            ),
        }
    ]


@pytest.fixture
def string_functions(spark_session):
    a, b = map(F.col, 'ab')
    ref_a, ref_b = map(ColumnReference, 'ab')
    return [
        {
            'col': F.concat(a, b),
            'expected': Function(name='concat', args=(ref_a, ref_b)),
            'values': {'a': 'a', 'b': 'b'},
        },
        {
            'col': F.concat_ws('-', a, b),
            'expected': Function(name='concat_ws', args=(Literal('-'), ref_a, ref_b)),
            'values': {'a': 'a', 'b': 'b'},
        },
        {
            'col': F.length(a),
            'expected': Function(name='length', args=(ref_a,))
        },
    ]


@pytest.fixture
def array_functions(spark_session):
    ref_a, ref_b = map(ColumnReference, 'ab')
    return [
        {
            'col': F.size('a'),
            'expected': Function(name='size', args=(ref_a,)),
            'values': {'a': [1, 2]},
            'schema': [T.StructField('a', T.ArrayType(T.IntegerType()))],
        },
    ]

@pytest.fixture
def aggregate_functions(spark_session):
    ref_a, _ = map(ColumnReference, 'ab')
    return [
        {
            'col': F.avg('a'),
            'expected': Function(name='avg', args=(ref_a,)),
        },
        {
            'col': F.count('a'),
            'expected': Function(name='count', args=(ref_a,)),
        },
        {
            'col': F.max('a'),
            'expected': Function(name='max', args=(ref_a,)),
        },
        {
            'col': F.min('a'),
            'expected': Function(name='min', args=(ref_a,)),
        },
        {
            'col': F.sum('a'),
            'expected': Function(name='sum', args=(ref_a,)),
        },
    ]

@pytest.fixture
def array_functions(spark_session):
    return [
        {
            'col': F.size('a'),
            'expected': Function(name='size', args=(ColumnReference('a'),)),
            'values': {'a': [1, 2]},
            'schema': [T.StructField('a', T.ArrayType(T.IntegerType()))],
        },
    ]


class Test_inspect_column:
    def test_core_functions(self, core_functions, subtests):
        tcs = core_functions
        for tc in tcs:
            with subtests.test(msg=str(tc['col'])):
                assert inspect_column(tc['col']) == tc['expected']

    def test_predicate_functions(self, predicate_functions, subtests):
        tcs = predicate_functions
        for tc in tcs:
            with subtests.test(msg=str(tc['col'])):
                assert inspect_column(tc['col']) == tc['expected']

    def test_operators(self, operators, subtests):
        tcs = operators
        for tc in tcs:
            with subtests.test(msg=str(tc['col'])):
                assert inspect_column(tc['col']) == tc['expected']

    def test_conditional_functions(self, conditional_functions, subtests):
        tcs = conditional_functions
        for tc in tcs:
            with subtests.test(msg=str(tc['col'])):
                assert inspect_column(tc['col']) == tc['expected']

    def test_string_functions(self, string_functions, subtests):
        tcs = string_functions
        for tc in tcs:
            with subtests.test(msg=str(tc['col'])):
                assert inspect_column(tc['col']) == tc['expected']

    def test_array_functions(self, array_functions, subtests):
        tcs = array_functions
        for tc in tcs:
            with subtests.test(msg=str(tc['col'])):
                assert inspect_column(tc['col']) == tc['expected']

    def test_aggregate_functions(self, aggregate_functions, subtests):
        tcs = aggregate_functions
        for tc in tcs:
            with subtests.test(msg=str(tc['col'])):
                assert inspect_column(tc['col']) == tc['expected']
