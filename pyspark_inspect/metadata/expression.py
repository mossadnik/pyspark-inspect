import pyspark_inspect.expression as E


class ExpressionVisitor:
    def visit(self, node: E.Expression) -> None:
        cls = type(node).__name__
        enter = getattr(self, f'visit_{cls}', None)
        expand = None
        if enter is not None:
            expand = enter(node)
        if expand or expand is None:
            for c in node.children:
                self.visit(c)
        leave = getattr(self, f'leave_{cls}', None)
        if leave is not None:
            leave(node)
        else:
            self.leave(node)

    def leave(self, node: E.Expression) -> None:
        pass


class ExpressionReferences(ExpressionVisitor):
    """Fetch source attributes for expression."""
    def __init__(self):
        self.references = set()

    def visit_AttributeReference(self, node: E.AttributeReference) -> bool:
        self.references.add(node)
        return False
