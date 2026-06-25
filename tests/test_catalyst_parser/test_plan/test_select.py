"""All classes that correspond to select

- Project
- Window
- Generate
"""


from typing import cast
import pytest
from pyspark_inspect.parser.catalyst import load_catalyst_plan, parse_plan
import pyspark_inspect.plan as P


class Test_Window:
    @pytest.fixture(scope='class')
    def window(self, parsed_sql):
        sql = """
            select a, c, sum(b) over (partition by c) as x
            from (select 1 as a, 2 as b, 3 as c)
        """
        return load_catalyst_plan(parsed_sql.get(sql)).children[0].children[0]

    def test_returns_Project(self, window):
        actual = parse_plan(window)
        assert isinstance(actual, P.Project)

    def test_adds_window_expressions_to_project_list(self, window):
        actual = cast(P.Project, parse_plan(window))
        assert {getattr(c, 'name') for c in actual.columns} == {'a', 'b', 'c', 'x'}
