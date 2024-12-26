def canon(triples):
    # algo seems to gets stuck (slow?)
    # wait for update TODO
    from pyoxigraph import Dataset, CanonicalizationAlgorithm
    def _(triples):
        d = Dataset(g.Quad(*t) for t in triples)
        d.canonicalize(CanonicalizationAlgorithm.UNSTABLE) # ?? unstable??
        for q in d: yield q.triple
    return tuple(_(triples))


from pyoxigraph import Quad
from typing import Iterable
def canon(quads: Iterable[Quad]) -> Iterable[Quad]:
    from  pyoxigraph import BlankNode
    from .data import index
    for i,itriples in index(quads).items():
        if isinstance(i.graph, BlankNode):
            raise ValueError(f'not handling graph blank/anon node')
        g = triples(itriples)
        if not i.nestedpredicate:
            yield from (Quad(*t, i.graph) for t in g)
        else:
            assert(i.nestedpredicate)
            yield from (Quad(t.subject, i.nestedpredicate, t.object) for t in g)


def deanon(
        quads: Iterable[Quad],
        anon_uri = "urn:anon:hash:") -> Iterable[Quad]:
    #for q in
    ...




def triples(ts):
    if not isinstance(ts, (list, tuple, set, frozenset)):
        ts = frozenset(ts)
    if not hasanon(ts):
        return ts
    
    from . import conversions as c
    ts = c.oxigraph.rdflib(ts)
    from rdflib import Graph
    ts = Graph().addN(ts)
    from rdflib.compare import to_canonical_graph
    ts = to_canonical_graph(ts)
    ts = c.rdflib.oxigraph(ts)
    return ts

def hasanon(d: Iterable[Quad]):
    from pyoxigraph import BlankNode
    for q in d:
        if isinstance(q.subject, BlankNode):
            return True
        if isinstance(q.object, BlankNode):
            return True
    return False

