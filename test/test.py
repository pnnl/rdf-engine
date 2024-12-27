
def quads(n=1, *, graph=None, anon=False, nested=False):
    from pyoxigraph import Quad, Triple
    from pyoxigraph import BlankNode, NamedNode
    for i in range(n):
        if anon:
            _ = Triple(BlankNode(), NamedNode(f'p:{i}'), BlankNode())
        else:
            _ = Triple(NamedNode(f's:{i}'), NamedNode(f'p:{i}'), NamedNode(f'o:{i}'))
        if nested:
            _ = Triple(_, NamedNode('m:'), _)
        yield Quad(*_,  NamedNode(f'g:{graph}') if graph else None)


def test():
    from rdf_engine import Engine
    r1 = frozenset( quads() )
    def r(s):
        return r1
    #s = Engine(rules=[r]).run()
    for s in Engine(rules=[r]): print(len((s)))
    assert(len(s) == len(r1))

