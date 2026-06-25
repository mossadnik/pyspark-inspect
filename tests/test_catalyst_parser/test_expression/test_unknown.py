from pyspark_inspect.parser.catalyst import parse_expression, CatalystPlan
import pyspark_inspect.expression as E


class Test_UnknownExpression:
    def test_parses_any_class_name(self):
        plan = CatalystPlan(children=[], data={'class': 'unknown-class'})
        actual = parse_expression(plan)
        assert isinstance(actual, E.UnknownExpression)
        assert actual.class_name == plan.class_name
        assert actual.args == ()
