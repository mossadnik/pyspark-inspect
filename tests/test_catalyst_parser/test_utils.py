from pyspark_inspect.parser.catalyst import parse_array_string


class Test_parse_array_string:
    def test_extracts_comma_separated_values(self):
        s = '[a, b]'
        expected = ('a', 'b')
        assert parse_array_string(s) == expected

    def test_returns_empty_tuple_if_input_empty(self):
        s = ''
        assert parse_array_string(s) == ()
