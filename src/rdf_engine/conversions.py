"""
just to use rdflib to canonicalize
"""
#use oxrdflib?
#also deal with strttl

class terms:
    def _():
        from rdflib.graph   import DATASET_DEFAULT_GRAPH_ID                   as DG
        from rdflib.term    import BNode     as BN, Literal as Lit, URIRef    as NN
        return locals()
    rdflib = _()
    def _():
        from pyoxigraph     import BlankNode as BN, Literal as Lit, NamedNode as NN
        from pyoxigraph     import DefaultGraph                               as DG; DG = DG()
        return locals()
    oxigraph = _()
    from types import SimpleNamespace as NS
    rdflib    = NS(**rdflib)
    oxigraph  = NS(**oxigraph)
    assert(frozenset(rdflib.__dict__.keys()) == frozenset(rdflib.__dict__.keys()))
    del NS

    class og2rl:
        def __call__(slf, n):
            rl = terms.rdflib
            og = terms.oxigraph
            if isinstance(n,
                        og.BN):
                return  rl.BN(n.value)
            elif isinstance(n,
                        og.Lit):
                return  rl.Lit(n.value,
                            datatype=None if n.language else slf(n.datatype),
                            #TypeError: A Literal can only have one of lang or datatype, per http://www.w3.org/TR/rdf-concepts/#section-Graph-Literal
                            lang=n.language)
            elif n ==   og.DG:
                return  rl.DG
            else:
                assert(isinstance(n, 
                        og.NN))
                return  rl.NN(n.value)
    og2rl = og2rl()

    class rl2og:
        def __call__(s, n):
            rl = terms.rdflib
            og = terms.oxigraph
            assert(isinstance(n, str)) # makes the following work for bn and nn
            if isinstance(n,
                        rl.BN):
                return  og.BN(n)
            elif isinstance(n,
                        rl.Lit):
                return  og.Lit(n,
                            datatype=s(n.datatype) if n.datatype else None,
                            language=n.language)
            if n ==     rl.DG:
                return  og.DG
            else:
                assert(isinstance(n,
                        rl.NN))
                return  og.NN(n)
    rl2og = rl2og()
terms = terms()


class types:
    from typing import Union
    rdflib =   Union[tuple(t for t in terms.rdflib  .__dict__.values() if t is not terms.rdflib  .DG)]
    oxigraph = Union[tuple(t for t in terms.oxigraph.__dict__.values() if t is not terms.oxigraph.DG)]

class rdflib:
    class oxigraph:
        from typing import Iterable
        from pyoxigraph import Triple, Quad
        def __call__(slf, d: Iterable[Iterable[types.oxigraph]]) -> Iterable[Iterable[types.rdflib]]:
            def _(q):
                _ = tuple(terms.rl2og(t) for t in q)
                if len(_) == 3: return slf.Triple(*_)
                else:
                    assert(len(_) == 4)
                    return slf.Quad(*_)
            _ = map(_, d)
            return _
            from pyoxigraph import serialize, RdfFormat
            _ = serialize(og, format=RdfFormat.N_QUADS)
            return _
            r = Dataset()
            r.parse(data=_, format='application/n-quads')
            return r
    oxigraph = oxigraph()
rdflib = rdflib()

class oxigraph:
    class rdflib:
        from typing import Iterable
        def __call__(slf, d: Iterable[Iterable[types.rdflib]]) -> Iterable[Iterable[types.oxigraph]]:
            # for putting it back in og
            _ = lambda q: tuple(terms.og2rl(t) for t in q)
            _ = map(_, d)
            return _
            _ = rl.serialize(format='application/n-quads')
            from pyoxigraph import parse, RdfFormat
            _ = parse(_, format=RdfFormat.N_QUADS)
            return _
    rdflib = rdflib()
oxigraph = oxigraph()
