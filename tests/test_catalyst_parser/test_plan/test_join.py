from typing import cast
import pytest
from pyspark_inspect.parser.catalyst import load_catalyst_plan, parse_plan
import pyspark_inspect.plan as P
import pyspark_inspect.expression as E


@pytest.fixture(scope='module')
def inner_join(parsed_sql):
    sql = """
        select *
        from (select 1 as a) s
        inner join (select 1 as b) t
        on s.a = t.b
    """
    return load_catalyst_plan(parsed_sql.get(sql)).children[0]


class Test_Join:
    def test_returns_Join(self, inner_join):
        actual = parse_plan(inner_join)
        assert isinstance(actual, P.Join)

    def test_captures_join_type(self, inner_join):
        actual = cast(P.Join, parse_plan(inner_join))
        assert actual.how == 'inner'

    def test_captures_join_condition(self, inner_join):
        actual = cast(P.Join, parse_plan(inner_join))
        assert isinstance(actual.on, E.Equals)
        assert set(cast(E.AttributeReference, c).name for c in actual.on.children) == {'a', 'b'}
