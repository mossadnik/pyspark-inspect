import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope='session')
def spark_session():
    return SparkSession.builder.appName('pyspark-inspect-tests').getOrCreate()
