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

