import pyspark_inspect.column.expression as E
from pyspark_inspect.column.expression import FunctionName as F

a, b, c = map(E.ColumnReference, 'abc')
number = E.Literal(1)
text = E.Literal('a')
boolean = E.Literal(True)

function_test_cases = [
    # operators
    (F.eq, (a, b)),
    (F.not_, (a,)),
    (F.and_, (a, b)),
    (F.or_, (a, b)),
    (F.add, (a, b)),
    (F.mul, (a, b)),
    (F.sub, (a, b)),
    (F.truediv, (a, b)),

    # predicate functions
    (F.isnull, (a,)),
    (F.isnotnull, (a,)),

    # conditional functions
    (F.coalesce, (a, b)),

    # string functions
    (F.concat, (a, b)),
    (F.concat_ws, (text, a, b)),
    (F.decode_, (a, text)),
    (F.encode_, (a, text)),
    (F.endswith_, (a, text)),
    (F.format_string, (text, a, b)),
    (F.length, (a,)),
    (F.lpad, (a, number, text)),
    (F.lower_, (a,)),
    (F.regexp_extract, (a, text, number)),
    (F.regexp_replace, (a, text, text)),
    (F.replace_, (a, text, text)),
    (F.rpad, (a, number, text)),
    (F.split_, (a, text, number)),
    (F.startswith_, (a, text)),
    (F.substring, (a, number, number)),
    (F.substring_index, (a, text, number)),
    (F.trim, (a,)),
    (F.upper_, (a,)),

    # datetime functions
    (F.current_date, ()),
    (F.current_timestamp, ()),
    (F.from_unixtime, (a, E.Literal('yyyy-MM-dd HH:mm:ss'))),
    (F.date_add, (a, number)),
    (F.date_format, (a, text)),
    (F.datediff, (a, b)),
    (F.month, (a,)),
    (F.timestamp_millis, (a,)),
    (F.to_date, (a,)),
    (F.unix_millis, (a,)),
    (F.year, (a,)),

    # mathematical functions
    (F.abs_, (a,)),
    (F.greatest, (a, b)),
    (F.hex, (a,)),
    (F.least, (a, b)),
    (F.negative, (a,)),
    (F.round, (a, number)),

    # hash functions
    (F.md5, (a,)),
    (F.xxhash64, (a,)),

    # aggregate functions
    (F.avg, (a,)),
    (F.count_, (a,)),
    (F.count_distinct, (a, b)),
    (F.collect_list, (a,)),
    (F.collect_set, (a,)),
    (F.first, (a, boolean)),
    (F.max_, (a,)),
    (F.max_by, (a, b)),
    (F.min_, (a,)),
    (F.sum_, (a,)),

    # collection functions
    (F.element_at, (a, number)),

    # array functions
    (F.array, (a, b)),
    (F.array_compact, (a,)),
    (F.array_contains, (a, b)),
    (F.array_distinct, (a,)),
    (F.array_intersect, (a, b)),
    (F.array_union, (a, b)),
    (F.flatten, (a,)),
    (F.size, (a,)),
    (F.sort_array, (a, boolean)),

    # struct functions
    (F.named_struct, (E.Literal('a'), a, E.Literal('x'), b)),

    # window functions
    (F.lag, (a, number, text)),
    (F.lead, (a, number, text)),
    (F.rank, ()),
    (F.row_number, ()),

    # generator functions
    (F.explode, (a,)),
    (F.explode_outer, (a,))
]
