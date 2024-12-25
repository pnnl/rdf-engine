from pyoxigraph import Store, Quad
from rdflib import Dataset
from typing import Iterable

#use oxrdflib?
#also deal with strttl

def og2rl(og: Store| Iterable[Quad] ) -> Dataset:
    # for doing things in rdflib
    """
    >>> from test import quads
    >>> q = quads(n=1000)
    >>> q = frozenset(q)
    >>> ds = og2rl(q)
    >>> len(ds) == len(q)
    True
    """
    from pyoxigraph import serialize, RdfFormat
    _ = serialize(og, format=RdfFormat.N_QUADS)
    r = Dataset()
    r.parse(data=_, format='application/n-quads')
    return r


class term:
    from rdflib.term    import BNode     as rlBN, Literal as rlLit, URIRef as    rlNN
    from pyoxigraph     import BlankNode as ogBN, Literal as ogLit, NamedNode as ogNN
    from rdflib.graph   import DATASET_DEFAULT_GRAPH_ID                       as rlDG
    from pyoxigraph     import DefaultGraph                                   as ogDG; ogDG = ogDG()
    class rl2og:
        def __call__(s, n):
            t = term
            assert(isinstance(n, str)) # makes the following work for bn and nn
            if isinstance(n,
                        t.rlBN):
                return  t.ogBN(n)
            elif isinstance(n,
                        t.rlLit):
                return  t.ogLit(n,
                            datatype=s(n.datatype) if n.datatype else None,
                            language=n.language)
            else:
                assert(isinstance(n,
                        t.rlNN))
                if n is t.rlDG: return t.ogDG
                return  t.ogNN(n)
    rl2og = rl2og()
    class og2rl:
        def __call__(s, n):
            t = term
            if isinstance(n,
                        t.ogBN):
                return  t.rlBN(n.value)
            elif isinstance(n,
                        t.ogLit):
                return  t.rlLit(n.value,
                            datatype=s(n.datatype),
                            lang=n.language)
            else:
                assert(isinstance(n, 
                        t.ogNN))
                if n is t.ogDG: return t.rlDG
                return  t.rlNN(n.value)
    og2rl = og2rl()
term = term()


def rl2og(rl: Dataset) -> Iterable[Quad]:
    # for putting it back in og
    """
    >>> from rdflib import Dataset
    >>> from test import quads
    >>> rl = og2rl(quads(n=1000))
    >>> og = rl2og(rl)
    >>> og = frozenset(og)
    >>> len(rl) == len(og)
    True
    """
    _ = rl.serialize(format='application/n-quads')
    from pyoxigraph import parse, RdfFormat
    _ = parse(_, format=RdfFormat.N_QUADS)
    return _

