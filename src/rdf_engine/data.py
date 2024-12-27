from pyoxigraph import Quad, Triple # putting imports here for max performance
class _index:
    """to group quads"""
    from typing import NamedTuple
    class index(NamedTuple):
        from pyoxigraph import NamedNode, BlankNode
        outerpredicate:  NamedNode | None
        graph:           NamedNode | BlankNode

        from typing import Self
        @classmethod
        def quad(cls, q: Quad) -> Self:
            if isinstance(q.subject, Triple):
                if not isinstance(q.object, Triple):
                    raise ValueError(f'not handling nested subject without nested object of ({q})')
                op = q.predicate # i care about the predicate
            else:
                op = None
            return cls(
                outerpredicate = op,
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

from .db import Ingestable
from typing import Iterable
from pyoxigraph import Quad
def quads(i: Ingestable) -> Iterable[Quad]:
    from .db import ingest, Store
    yield from ingest(Store(), i)
