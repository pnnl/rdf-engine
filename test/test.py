
def json():
    return {}

def jsonloader():
    from json2rdf import j2r

def mapper(db):
    ...


def test():
    from engine.triples import (ConstructQuery,
                                 Rules, PyRule, noeffect_pyfunc, 
                                 Engine,
                                 Triples,
                                 OxiGraph)
    import pyoxigraph as g

    def _data(db: OxiGraph) -> Triples:
        _ = b'<http://example.com> <http://example.com/p> "1" .\n'
        _ = g.parse(_, 'text/turtle')
        _ = Triples(_)
        return _
    Data = PyRule(_data)
    _ = """
    construct {
        <http://example.com/a> <http://www.w3.org/2000/01/rdf-schelabel> "something" .}
    where {  }
    """
    Construct = ConstructQuery(_)
    rules = Rules([noeffect_pyfunc, Data, Construct])
    engine = Engine(rules, OxiGraph(g.Store()) )
    res = engine()
    _ = res.db
    _ = list(_)
    return _


def test_deanon():
    from engine.triples import deanon, flatten
    from pyoxigraph import Triple, NamedNode, BlankNode
    # test bn deanoned stay stay the same.
    x = BlankNode('x')
    p = NamedNode('p:')
    y = BlankNode('y')
    t = Triple(x, p, y)
    m = NamedNode('m:')
    i = [
        Triple(t, m , t),
        t 
        ]
    o = deanon(i)
    o = list(o)
    for n in flatten(o):
        assert(not isinstance(n, BlankNode) )
    assert(isinstance(o[0].subject, Triple) )
    assert(isinstance(o[0].object, Triple)  )
    assert(o[0].subject.subject     == o[1].subject  )
    assert(o[0].subject.object      == o[1].object  )
    assert(o[0].object.subject      == o[1].subject  )
    assert(o[0].object.object       == o[1].object  )
    assert(o[0].subject.predicate   == o[1].predicate  )
    assert(o[0].object.predicate    == o[1].predicate  )
    # test nn unchanged
    s = NamedNode('s:')
    o = NamedNode('o:')
    i = [
        Triple(s, p, o)
        ]
    do = deanon(i)
    do = list(do)
    assert(do[0].subject     == s)
    assert(do[0].predicate   == p)
    assert(do[0].object      == o)


def quads(n=1):
    from pyoxigraph import Quad, Triple
    from pyoxigraph import BlankNode, NamedNode
    for i in range(n):
        _ = Triple(BlankNode(), NamedNode('p:'), BlankNode())
        yield Quad(*_)


def insert(method, n=int(1e6)):
    # performance
    from pyoxigraph import Store
    s = Store()
    q = quads(n=n)
    if method == 'objects': 
        s.bulk_extend(q)
        # ~6s
    else:
        assert(method == 'serialized')
        from pyoxigraph import serialize, RdfFormat
        _ = serialize(q, format=RdfFormat.N_QUADS)
        s.bulk_load(_,   format=RdfFormat.N_QUADS)
        # ~9s for ntriples and ttl
