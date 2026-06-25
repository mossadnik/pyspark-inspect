import json
from textwrap import dedent
from pathlib import Path
import pytest
from pyspark_inspect.parser.catalyst import CatalystPlan
from pyspark.sql import SparkSession


@pytest.fixture(scope='session')
def spark():
    return SparkSession.builder.appName('pyspark-inspect-tests').getOrCreate()


@pytest.fixture
def simple_table_plan():
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


class IndentDict:
    def __init__(self, data: dict):
        self._data = data

    def get(self, key: str):
        k = dedent(key).strip('\n')
        return self._data[k]


@pytest.fixture(scope='session')
def parsed_sql():
    root = Path(__file__).parent
    with open(root / 'sql-plan.json') as f:
        data = json.load(f)
    return IndentDict(data)
