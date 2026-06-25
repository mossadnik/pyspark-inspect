"""All classes that correspond to select

- Project
- Window
- Generate
"""


from typing import cast
import pytest
from pyspark_inspect.parser.catalyst import load_catalyst_plan, parse_plan
import pyspark_inspect.plan as P
from pyspark_inspect.expression import Expression

class Test_Window:
    @pytest.fixture(scope='class')
    def filter_(self, parsed_sql):
        sql = """
            select *
            from (select 1 as a)
            where a = 1
        """
        return load_catalyst_plan(parsed_sql.get(sql)).children[0]

    def test_returns_Filter(self, filter_):
        actual = parse_plan(filter_)
        assert isinstance(actual, P.Filter)

    def test_captures_condition(self, filter_):
        actual = cast(P.Filter, parse_plan(filter_))
        assert isinstance(actual.condition, Expression)
