
def quads(n=1, *,
            rand=False, anon=False,
            graph=None,
            nested=False,):
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
            #_ = Triple(BlankNode(), p, p )  
            #_ = Triple(p, p, BlankNode(),) 
            _ = Triple(BlankNode(), p, BlankNode(),) # 
        else:
            if rand:
                _ = Triple(NamedNode(f's:{BlankNode().value}'), p, NamedNode(f'o:{BlankNode().value}'))
            else:
                _ = Triple(NamedNode(f's:{i}'), p, NamedNode(f'o:{i}'))
        return _
    
    for i in range(n):
        if nested:
            _ = triple()
            predicate = NamedNode('http://www.w3.org/1999/02/22-rdf-syntax-ns#reifies')
            # https://github.com/oxigraph/oxigraph/issues/1472
            id = _.subject
            id = BlankNode()
            _ = Triple(id, predicate, triple())
            yield Quad(*_.object,  NamedNode(f'g:{graph}') if graph else None)
            yield Quad(*_,         NamedNode(f'g:{graph}') if graph else None)
            #_ = Triple(id, _.predicate, triple()) # not handled
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



def args():
    from itertools import product
    class n(list): ...
    n = n([100,  ])
    class graph(list): ...
    graph = graph(['x', 'y' ])
    class anon(list): ...
    anon = anon([ False, True ])
    class nested(list): ...
    nested = nested([ True ])
    class rand(list): ...
    rand = rand([False])

    # class n(list): ...
    # n = n([100,  ])
    # class graph(list): ...
    # graph = graph(['x',  ])
    # class anon(list): ...
    # anon = anon([ True])
    # class nested(list): ...
    # nested = nested([True ])
    # class rand(list): ...
    # rand = rand([False])

    argsl = n, anon, rand, nested, graph
    _ = product(*argsl)
    return [{ argsl[i].__class__.__name__:a for i,a in enumerate(args) } for args in (_)]
def rules():
    return [Quads(**kw) for kw in args() ]

def test_cycling():
    rs = rules()
    n = 5
    from rdf_engine import Engine
    e = Engine(rules=rs, MAX_NCYCLES=n,
               derand='canonicalize',
               #log_debug=True,
                )
    s = e.run()
    print(len((s)))
    assert(e.i <n)
    #assert(len(s) == sum(len(frozenset(r('_'))) for r in rules() )   )
    print(frozenset(t.graph_name for t in s))


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
