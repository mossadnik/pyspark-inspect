from typing import cast
import pytest
from pyspark_inspect.parser.catalyst import load_catalyst_plan, parse_plan
import pyspark_inspect.plan as P


@pytest.fixture(scope='module')
def union_all(parsed_sql):
    sql = """
        select 1 as a
        union all
        select 2 as a
    """
    return load_catalyst_plan(parsed_sql.get(sql))


@pytest.fixture(scope='module')
def except_all(parsed_sql):
    sql = """
        select 1 as a
        except all
        select 2 as a
    """
    return load_catalyst_plan(parsed_sql.get(sql))


@pytest.fixture(scope='module')
def intersect_all(parsed_sql):
    sql = """
        select 1 as a
        intersect all
        select 2 as a
    """
    return load_catalyst_plan(parsed_sql.get(sql))


class Test_Union:
    def test_returns_Union(self, union_all):
        actual = parse_plan(union_all)
        assert isinstance(actual, P.Union)

    def test_captures_children(self, union_all):
        actual = cast(P.Union, parse_plan(union_all))
        assert isinstance(actual.left, P.Project)
        assert isinstance(actual.right, P.Project)

    def test_captures_parameters(self, union_all):
        actual = cast(P.Union, parse_plan(union_all))
        assert not actual.by_name
        assert not actual.allow_missing_columns


class Test_Except:
    def test_returns_Except(self, except_all):
        actual = parse_plan(except_all)
        assert isinstance(actual, P.Except)

    def test_captures_children(self, except_all):
        actual = cast(P.Except, parse_plan(except_all))
        assert isinstance(actual.left, P.Project)
        assert isinstance(actual.right, P.Project)

    def test_captures_parameters(self, except_all):
        actual = cast(P.Except, parse_plan(except_all))
        assert actual.is_all


class Test_Intersect:
    def test_returns_Intersect(self, intersect_all):
        actual = parse_plan(intersect_all)
        assert isinstance(actual, P.Intersect)

    def test_captures_children(self, intersect_all):
        actual = cast(P.Intersect, parse_plan(intersect_all))
        assert isinstance(actual.left, P.Project)
        assert isinstance(actual.right, P.Project)

    def test_captures_parameters(self, intersect_all):
        actual = cast(P.Intersect, parse_plan(intersect_all))
        assert actual.is_all
