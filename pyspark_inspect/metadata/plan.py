import pyspark_inspect.plan as P


class PlanVisitor:
    def visit(self, node: P.Plan) -> None:
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

    def leave(self, node: P.Plan) -> None:
        pass
