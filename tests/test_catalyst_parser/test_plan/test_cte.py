from typing import cast
import pytest
from pyspark_inspect.parser.catalyst import load_catalyst_plan, parse_plan
import pyspark_inspect.plan as P


@pytest.fixture(scope='module')
def simple(parsed_sql):
    sql = """
        with t as (select 1 as a)
        select * from t
    """
    return load_catalyst_plan(parsed_sql.get(sql))


class Test_CTERelationDef:
    def test_returns_CTERelationDef(self, simple):
        actual = parse_plan(simple.children[0])
        assert isinstance(actual, P.CTEDef)


class Test_WithCTE:
    def test_returns_WithCTE(self, simple):
        actual = parse_plan(simple)
        assert isinstance(actual, P.WithCTE)


class Test_CTERelationRef:
    def test_returns_CTERelationRef(self, simple):
        select = parse_plan(simple.children[1])
        from_table_alias = select.children[0]
        cte_ref = from_table_alias.children[0]
        assert isinstance(cte_ref, P.CTERef)
        assert isinstance(cast(P.CTERef, cte_ref).cte_id, int)
