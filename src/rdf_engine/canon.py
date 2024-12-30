"""
canonicalization
"""

class _quads:
    from pyoxigraph import Quad
    from typing import Iterable
    def __call__(self, quads: Iterable[Quad]) -> Iterable[Quad]:
        """
        canonicalization of sets of quads
        """
        # the set of quads have to be broken into sets of triples
        # after canonicalization,
        # each set of triples has to be reassembled as quads
        from  pyoxigraph import BlankNode
        from .data import index
        for i,itriples in index(quads).items():
            if isinstance(i.graph, BlankNode):
                raise ValueError(f'not handling graph blank/anon node of graph {i.graph}')
            if not i.outerpredicate:
                c = triples(itriples)
                yield from (self.Quad(*t, i.graph) for t in c)
            else:
                assert(i.outerpredicate)
                # separately to keep computations down.
                # assumption: the nested triple terms are not related anonymously
                cs = triples(t.subject  for t in itriples)
                assert(len(itriples))
                co = triples(t.object   for t in itriples)
                assert(len(cs) == len(co))
                for s in cs:
                    for o in co:
                        yield self.Quad(s, i.outerpredicate, o, i.graph)

    class _deanon:
        from pyoxigraph import Triple
        from typing import Iterable
        from pyoxigraph import Quad
        def __call__(slf,
                quads: Iterable[Quad], *,
                uri = "urn:anon:hash:") -> Iterable[Quad]:
            """takes blank node value as an identifier for a uri"""
            _ = map(lambda q: slf.quad(q, uri), quads)
            return _
        @classmethod
        def quad(cls, q: Quad, uri):
            if isinstance(q.subject, cls.Triple):
                _ = cls.Triple(*(cls.f(n, uri) for n in q.subject))
                q = cls.Quad(_, q.predicate, q.object, q.graph_name)
            if isinstance(q.object, cls.Triple):
                _ = cls.Triple(*(cls.f(n, uri) for n in q.object))
                q = cls.Quad(q.subject, q.predicate, _, q.graph_name)
            return cls.Quad(*(cls.f(n, uri) for n in q))
        
        from pyoxigraph import BlankNode, NamedNode
        @classmethod
        def f(cls, n, uri: str):
            if isinstance(n, cls.BlankNode):
                return cls.NamedNode(uri+n.value)
            else:
                return n
    deanon = _deanon()
quads = _quads()


from pyoxigraph import BlankNode, Quad, Triple
from typing import Iterable
def hasanon(d: Iterable[Quad| Triple]):
    for q in d:
        if isinstance(q.subject, BlankNode):
            return True
        if isinstance(q.object, BlankNode):
            return True
    return False


def triples(ts):
    if not isinstance(ts, frozenset):
        ts = frozenset(ts)
    assert(isinstance(ts, frozenset))
    from pyoxigraph import Triple
    for t in ts:
        assert(not isinstance(t.subject,    Triple))
        assert(not isinstance(t.object,     Triple))
    # optimization
    if not hasanon(ts):
        return ts
    
    from . import conversions as c
    ts = c.oxigraph.rdflib(ts)
    from rdflib import Graph
    _ = Graph()
    for t in ts: ts = _.add(t)
    from rdflib.compare import to_canonical_graph
    ts = _; del _
    ts = to_canonical_graph(ts)
    ts = c.rdflib.oxigraph(ts)
    ts = frozenset(ts)
    return ts
def _ogtriples(triples):
    # algo seems to gets stuck (slow?)
    # wait for update TODO
    from pyoxigraph import Dataset, CanonicalizationAlgorithm, Quad
    def _(triples):
        d = Dataset(Quad(*t) for t in triples)
        d.canonicalize(CanonicalizationAlgorithm.UNSTABLE) # ?? unstable??
        for q in d: yield q.triple
    yield from _(triples)
