# pyspark-inspect

pyspark-inspect allows to inspect and manipulate pyspark Column expressions across different spark versions.

## Compatibility & Limitiations

- Tested against Spark 3.5 Classic and Connect, Spark 4.3 Connect
- No full coverage

## Basic Usage

```python
from pypspark.sql import functions as F
from pyspark_inspect import inspect_column
from pyspark_inspect.column import expression as E

ast = inpect_column(F.col('a') + F.col('b'))
col = ast.to_column()
```
