from collections.abc import Callable, Generator
from dataclasses import Field, dataclass, field, fields

__all__ = [
    'Term',
    'child',
    'evolve',
    'param',
    'rewrite',
]


@dataclass(frozen=True)
class Term:
    """A term with children and parameters that are not part of term tree."""

    def _child_fields(self) -> list[str]:
        return [f.name for f in fields(self) if _is_child_field(f)]

    def _param_fields(self) -> list[str]:
        return [f.name for f in fields(self) if not _is_child_field(f)]

    @property
    def children(self) -> 'dict[str, Term | tuple[Term, ...]]':
        return {name: getattr(self, name) for name in self._child_fields()}

    @property
    def params(self) -> 'dict[str, Term | tuple[Term, ...]]':
        return {name: getattr(self, name) for name in self._param_fields()}

    def iter_children(self) -> 'Generator[Term, None, None]':
        """Return an flat iterator over all children."""
        for v in self.children.values():
            if isinstance(v, tuple):
                yield from v
            else:
                yield v


def evolve(term: Term, children: dict[str, Term | tuple[Term, ...]]) -> Term:
    """Return a new term with updated children.

    - Children that are not provided are taken from the original term.
    - All params have the same value as in the original term.
    """
    kwargs = {}
    for f in fields(term):
        if f.name in children:
            kwargs[f.name] = children[f.name]
        else:
            kwargs[f.name] = getattr(term, f.name)
    return term.__class__(**kwargs)


def rewrite(term: Term, rule: Callable[[Term], Term | None]) -> Term:
    """Apply rule to term with bottom-up strategy.

    Args:
        term: Term to rewrite.
        rule: Rule to apply. Should return `None` if no rewrite to be applied.
    """
    children = {}
    for name, child in term.children.items():
        if isinstance(child, Term):
            children[name] = rewrite(child, rule)
        elif isinstance(child, tuple):
            children[name] = tuple([rewrite(c, rule) for c in child])
    if children:
        term = evolve(term, children)
    rebuilt = rule(term)
    if rebuilt is None:
        rebuilt = term
    return rebuilt


IS_CHILD = '__is_term_child__'


def _is_child_field(f) -> bool:
    return f.metadata[IS_CHILD]


def child(**kwargs) -> Field:
    metadata = kwargs.pop('metadata', {})
    metadata[IS_CHILD] = True
    return field(**kwargs, metadata=metadata)


def param(**kwargs) -> Field:
    metadata = kwargs.pop('metadata', {})
    metadata[IS_CHILD] = False
    return field(**kwargs, metadata=metadata)
