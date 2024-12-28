
def quads(n=1, *, rand=False, graph=None, anon=False, nested=''):
    from pyoxigraph import Quad, Triple
    from pyoxigraph import BlankNode, NamedNode

    def ps(rand):
        if rand:
            p = f'p:{BlankNode().value}'
        else:
            p = f'p:{i}'
        return p
    def triple():
        p = NamedNode(ps(rand))
        if anon:
            _ = Triple(BlankNode(), p, BlankNode())
        else:
            if rand:
                _ = Triple(NamedNode(f's:{BlankNode().value}'), p, NamedNode(f'o:{BlankNode().value}'))
            else:
                _ = Triple(NamedNode(f's:{i}'), p, NamedNode(f'o:{i}'))
        return _
    
    for i in range(n):
        if nested:
            _ = Triple(triple(), NamedNode(f'm:{nested}'), triple())
        else:
            _ = triple()
        yield Quad(*_,  NamedNode(f'g:{graph}') if graph else None)


def ttl():
    return open('test.ttl').read()

def args():
    from itertools import product
    class n(list): ...
    n = n([int(1e3),  ])
    class graph(list): ...
    graph = graph([None, 'g', 'gg'])
    class anon(list): ...
    anon = anon([True, False])
    class nested(list): ...
    nested = nested([True, False])

    argsl = n, graph, anon, nested
    _ = product(*argsl)
    return [{ argsl[i].__class__.__name__:a for i,a in enumerate(args) } for args in (_)]
def rules():
    argss = args()
    data = [ frozenset(quads(**args)) for args in argss ]
    return [lambda _s: d for d in (data) ]

def test():
    from rdf_engine import Engine
    #rs =  [ lambda s: frozenset( quads(1000, anon=True, rand=False) ) ]
    #rs = [ lambda s: ttl() ]
    rs =  [ lambda s: frozenset( quads(100, anon=True, rand=False, nested=True) ), 
           lambda s: frozenset( quads(100, anon=True, rand=False, nested=True, graph='g') ),
            ]
    s = Engine(rules=rs, MAX_NCYCLES=5, canon=True, deanon=False ).run()
    print(len((s)))
    #assert(len(s) == sum(len(frozenset(r('_'))) for r in rules() )   )


if __name__ == '__main__':
    from rdf_engine import logger
    import logging
    logging.basicConfig(force=True) # force removes other loggers that got picked up.
    logger.setLevel(logging.INFO)
    test()

