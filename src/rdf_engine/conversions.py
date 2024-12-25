from pyoxigraph import Store, Quad
from rdflib import Dataset
from typing import Iterable


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


class node:
    from rdflib     import BNode     as rlBN, Literal as rlLit, URIRef as    rlNN
    from pyoxigraph import BlankNode as ogBN, Literal as ogLit, NamedNode as ogNN
    class rl2og:
        def __call__(s, n):
            s = node
            assert(isinstance(n, str)) # makes the following work for bn and nn
            if isinstance(n,
                        s.rlBN):
                return  s.ogBN(n)
            elif isinstance(n,
                        s.rlLit):
                return  s.ogLit(n, datatype=s(n.datatype) if n.datatype else None)
            else:
                assert(isinstance(n,
                        s.rlNN))
                return  s.ogNN(n)
    rl2og = rl2og()
    class og2rl:
        def __call__(s, n):
            s = node
            if isinstance(n,
                        s.ogBN):
                return  s.rlBN(n.value)
            elif isinstance(n,
                        s.ogLit):
                return  s.rlLit(n.value)
            else:
                assert(isinstance(n, 
                        s.ogNN))
                return  s.rlNN(n.value)
    og2rl = og2rl()
node = node()


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

