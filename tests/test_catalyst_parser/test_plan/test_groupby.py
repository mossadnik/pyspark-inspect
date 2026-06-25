"""All classes that relate to group by

- Aggregate
"""


from typing import cast
import pytest
from pyspark_inspect.parser.catalyst import load_catalyst_plan, parse_plan
import pyspark_inspect.plan as P


class Test_Aggregate:
    @pytest.fixture(scope='class')
    def aggregate(self, parsed_sql):
        sql = """
            select a, count(*) as b
            from (select 1 as a)
            group by a
        """
        return load_catalyst_plan(parsed_sql.get(sql))

    def test_returns_Aggregate(self, aggregate):
        actual = parse_plan(aggregate)
        assert isinstance(actual, P.Aggregate)

    def test_resolves_grouping_expressions(self, aggregate):
        actual = cast(P.Aggregate, parse_plan(aggregate))
        assert {getattr(c, 'name') for c in actual.grouping_expressions} == {'a',}

    def test_resolves_columns(self, aggregate):
        actual = cast(P.Aggregate, parse_plan(aggregate))
        assert {getattr(c, 'name') for c in actual.columns} == {'a', 'b'}
