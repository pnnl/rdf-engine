from pyoxigraph import Quad, Triple # putting imports here for max performance
class _index:
    """to group quads"""
    from typing import NamedTuple
    class index(NamedTuple):
        from pyoxigraph import NamedNode, BlankNode
        nestedpredicate:  NamedNode | None
        graph:   NamedNode

        from typing import Self
        @classmethod
        def quad(cls, q: Quad) -> Self:
            if isinstance(q.subject, Triple):
                if not isinstance(q.object, Triple):
                    raise ValueError('not handling nested subject without nested object')
                np = q.predicate # i care about the predicate
            else:
                np = None
            return cls(
                nestedpredicate = np,
                graph = q.graph_name
            )
    
    from typing import Iterable
    from pyoxigraph import Quad
    def __call__(slf, d: Iterable[Quad]) -> dict[index, frozenset[Triple]]:
        from collections import defaultdict
        idx = defaultdict(set)
        for q in d: idx[slf.index.quad(q)].add(q.triple)
        for k,v in idx.items(): idx[k] = frozenset(v)
        return idx

index = _index()
