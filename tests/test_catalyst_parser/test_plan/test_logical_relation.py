from typing import cast
from pyspark.sql import types as T
from pyspark_inspect.parser.catalyst import CatalystPlan, parse_plan
import pyspark_inspect.plan as P


class Test_parse_LogicalRelation:
    def simple_plan(self):
        return CatalystPlan(
            children=[],
            data={
                'class': 'org.apache.spark.sql.execution.datasources.LogicalRelation',
                'relation': None,
                'output': [[
                    {
                        'class': 'org.apache.spark.sql.catalyst.expressions.AttributeReference',
                        'num-children': 0,
                        'name': 'a',
                        'dataType': 'string',
                        'exprId': {'id': 1},
                        'qualifier': '[]',
                    }
                ]],
                'catalogTable': {
                    'product-class': 'org.apache.spark.sql.catalyst.catalog.CatalogTable',
                    'identifier': {
                        'product-class': 'org.apache.spark.sql.catalyst.TableIdentifier',
                        'table': 'df',
                        'database': 'default',
                        'catalog': 'spark_catalog'
                    }
                }
            }
        )

    def test_returns_Table(self):
        """This may need to be qualified if LogicalRelation is used for other cases as well."""
        plan = self.simple_plan()
        actual = parse_plan(plan)
        assert isinstance(actual, P.Table)

    def test_parses_qualified_table_name(self):
        plan = self.simple_plan()
        expected = 'spark_catalog.default.df'
        actual = cast(P.Table, parse_plan(plan))
        assert actual.qualified_name == expected

    def test_parses_columns(self):
        plan = self.simple_plan()
        actual = cast(P.Table, parse_plan(plan))
        assert {getattr(c, 'name') for c in actual.columns} == {'a',}
