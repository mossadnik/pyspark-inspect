# pyspark-inspect

pyspark-inspect allows to inspect the query plans of pyspark dataframes from Python.

It converts analyzed Catalyst plans into Python data structures that can be queried programmatically.

## Compatibility & Limitiations

- Tested against Spark 3.5
- Spark Connect is not supported
- Coverage of Spark SQL operations is currently very limited

## Basic Usage

```python
from pypspark.sql import functions as F
from pyspark_inspect import inspect_dataframe

df = (
    spark.createDataFrame([[1, 2]], ['a', 'b'])
    .withColumn('c', F.col('a') + F.col('b'))
)
plan = inspect_dataframe(df)
```
