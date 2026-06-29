import pyspark_inspect.expression as E
from pyspark_inspect.metadata.expression import ExpressionReferences


class TestExpressionVisitor:
    def test_fetches_resolved_references(self):
        refs = [
            E.AttributeReference('a', (), 0),
            E.AttributeReference('b', (), 1)
        ]
        expr = E.UnknownExpression(
            args=(refs[0], E.Not(refs[1])),
            class_name='unknown'
        )
        visitor = ExpressionReferences()
        visitor.visit(expr)
        assert visitor.references == set(refs)
