from typing import cast
import pytest
from pyspark_inspect.parser.catalyst import CatalystPlan, parse_plan
import pyspark_inspect.plan as P


@pytest.fixture
def simple_plan(simple_table_plan):
    return CatalystPlan(
        children=[simple_table_plan],
        data={
            'class': 'org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias',
            'identifier': {
                'name': 'df',
                'qualifier': '[a, b]'
            },
        }

    )


class Test_parse_SubqueryAlias:
    def test_returns_Alias(self, simple_plan):
        """This may need to be qualified if LogicalRelation is used for other cases as well."""
        actual = parse_plan(simple_plan)
        assert isinstance(actual, P.Alias)

    def test_parses_alias(self, simple_plan):
        actual = cast(P.Alias, parse_plan(simple_plan))
        assert actual.alias == 'df'

    def test_parses_qualifier(self, simple_plan):
        actual = cast(P.Alias, parse_plan(simple_plan))
        assert actual.qualifier == ('a', 'b')
