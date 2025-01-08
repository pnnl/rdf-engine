
def quads(n=1, *,
            rand=False, anon=False,
            graph=None,
            nested='',):
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
            _ = Triple(triple(), NamedNode(f'm:{nested}'), triple(),)
        else:
            _ = triple()
        yield Quad(*_,  NamedNode(f'g:{graph}') if graph else None)

class Quads:
    def __init__(self, **kwargs):
        from types import SimpleNamespace as NS
        self.params = NS(**kwargs)
    def __call__(self, _):
        return quads(**self.params.__dict__)
    def __repr__(self):
        return repr(self.params).replace('namespace', 'Quads')


def ttl():
    return open('test.ttl').read()

def args():
    from itertools import product
    class n(list): ...
    n = n([100,  ])
    class graph(list): ...
    graph = graph(['x', 'y' ])
    class anon(list): ...
    anon = anon([ True])
    class nested(list): ...
    nested = nested(['meta', False ])
    class rand(list): ...
    rand = rand([False])

    argsl = n, anon, rand, nested, graph
    _ = product(*argsl)
    return [{ argsl[i].__class__.__name__:a for i,a in enumerate(args) } for args in (_)]
def rules():
    return [Quads(**kw) for kw in args() ]

def test():
    rs = rules()
    logging()
    from rdf_engine import Engine
    e = Engine(rules=rs, MAX_NCYCLES=5, derand='canonicalize')
    s = e.run()
    print(len((s)))
    #assert(len(s) == sum(len(frozenset(r('_'))) for r in rules() )   )
    print(frozenset(t.graph_name for t in s))


def logging():
    from rdf_engine import logger
    import logging
    logging.basicConfig(force=True) # force removes other loggers that got picked up.
    logger.setLevel(logging.INFO)

def test_prog():
    from pathlib import Path
    s = Path('program.yaml').read_text()
    from yaml import safe_load
    db = safe_load(s)['db']
    if Path(db).exists():
        from shutil import rmtree
        rmtree(db, )
    
    from rdf_engine.program import Program
    _ = Program.parse(s)
    _ = _.run()


if __name__ == '__main__':
    _ = test_prog()
    print(_)
