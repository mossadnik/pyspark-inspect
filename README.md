# pyspark-inspect

pyspark-inspect allows to inspect and manipulate pyspark Column expressions across different spark versions.

## Compatibility & Limitiations

There is only a subset of functions implemented.

Tested against:

<table>
<tr>
    <td rowspan="2">Spark Classic</td>
    <td>3.5.2</td>
    <td>Databricks Runtime 16.4</td>
</tr>
<tr>
    <td>3.5.9</td>
    <td>Apache Spark</td>
</tr>
<tr>
    <td rowspan="2">Spark Connect</td>
    <td>3.5.2</td>
    <td>Databricks Runtime 16.4</td>
</tr>
<tr>
    <td>4.2.0</td>
    <td>Databricks Serverless</td>
</tr>
</table>

## Basic Usage

```python
from pypspark.sql import functions as F
from pyspark_inspect import inspect_column
from pyspark_inspect.column import expression as E

ast = inpect_column(F.col('a') + F.col('b'))
col = ast.to_col()
```
