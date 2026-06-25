"""Other classes not covered elsewhere:

- Distinct
"""


import pytest
from pyspark_inspect.parser.catalyst import load_catalyst_plan, parse_plan
import pyspark_inspect.plan as P


class Test_Distinct:
    @pytest.fixture(scope='class')
    def distinct(self, parsed_sql):
        sql = """
            select distinct a
            from (select 1 as a, 2 as b)
        """
        return load_catalyst_plan(parsed_sql.get(sql))

    def test_returns_Distinct(self, distinct):
        actual = parse_plan(distinct)
        assert isinstance(actual, P.Distinct)


class Test_Limit:
    @pytest.fixture(scope='class')
    def limit(self, parsed_sql):
        sql = """
            select 1 as a limit 1
        """
        return load_catalyst_plan(parsed_sql.get(sql))

    def test_global_limit(self, limit):
        actual = parse_plan(limit)
        assert isinstance(actual, P.GlobalLimit)
        assert actual.limit == 1

    def test_local_limit(self, limit):
        actual = parse_plan(limit.children[0])
        assert isinstance(actual, P.LocalLimit)
        assert actual.limit == 1
