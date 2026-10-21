import datetime
import itertools as it
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


class Test_Literal:
    def assert_value_preserved(self, value):
        res = inspect_column(F.lit(value))
        assert isinstance(res, Literal)
        assert res.value == value

    def test_string_value(self, spark_session):
        self.assert_value_preserved('Abc123!')

    def test_bool_value(self, spark_session):
        self.assert_value_preserved(True)
        self.assert_value_preserved(False)

    def test_int_value(self, spark_session):
        self.assert_value_preserved(12345)

    def test_date_value(self, spark_session):
        self.assert_value_preserved(datetime.date(1999, 9, 9))

    def test_datetime_value(self, spark_session):
        self.assert_value_preserved(datetime.datetime(1999, 9, 9, 11, 11, 11, 123456))

    def test_None(self, spark_session):
        self.assert_value_preserved(None)


class Test_synonyms:
    """Sometimes synonyms are represented differently, check that they unified."""
    def assert_same(self, *cols):
        for this, that in it.pairwise(cols):
            assert inspect_column(this) == inspect_column(that)

    def test_isnotnull(self, spark_session):
        self.assert_same(
            F.isnotnull('a'),
            F.col('a').isNotNull()
        )

    def test_isnull(self, spark_session):
        self.assert_same(
            F.isnull('a'),
            F.col('a').isNull()
        )

    def test_neq(self, spark_session):
        self.assert_same(
            F.col('a') != F.col('b'),
            ~(F.col('a') == F.col('b')),
        )

    def test_struct(self, spark_session):
        self.assert_same(
            F.struct(F.col('a').alias('b'), F.col('x').alias('y')),
            F.named_struct(F.lit('b'), F.col('a').alias('b'), F.lit('y'), F.col('x').alias('y'))
        )


@pytest.fixture
def conditional_functions(spark_session):
    a, b = map(F.col, 'ab')
    ref_a, ref_b = map(ColumnReference, 'ab')
    return [
        {
            'col': F.coalesce(a, b),
            'expected': Function(name=FunctionName.coalesce, args=(ref_a, ref_b)),
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


class Test_inspect_column:
    def test_core_functions(self, core_functions):
        tcs = core_functions
        for tc in tcs:
            assert inspect_column(tc['col']) == tc['expected']

    def test_conditional_functions(self, conditional_functions):
        tcs = conditional_functions
        for tc in tcs:
            assert inspect_column(tc['col']) == tc['expected']
