
class _reification:
    from typing import Iterable
    from pyoxigraph import Triple, BlankNode
    class terms:
        prefix = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
        terms = {'Statement', 'type', 'reifies',
                 'subject', 'predicate', 'object'}
        def __init__(self):
            for t in self.terms:
                setattr(self, t, self.nn(self.prefix, t))
        @staticmethod
        def nn(prefix, term):
            from pyoxigraph import NamedNode
            return NamedNode(f"{prefix}{term}")
    terms = terms()
    def standard(self, str: Iterable[Triple], deterministic_nested=True) -> Iterable[Triple]:
        from .canon import quads
        uri = quads.deanon.defaults.uri
        T = self.Triple
        trm = self.terms
        old2new = {}
        str = frozenset(str)  # need two passes for deterministic_nested case

        def isnested(t):
            # nested/reifying  triple https://www.w3.org/TR/rdf12-concepts/#dfn-reifying-triple
            return (t.predicate == trm.reifies) and isinstance(t.object, T)
        for t in str:
            if isnested(t):
                ndt = t.object                            # always  here
                assert(not isinstance(t.subject, T))      # but not here
                # https://github.com/oxigraph/oxigraph/issues/1286
                # <<d:s d:p d:o>> m:p m:o .
                # <<d:s d:p d:o>> m:p2 m:o2 .
                # # not the same as
                # <<d:s d:p d:o>> m:p m:o; m:p2 m:o2 .
                # doesn't make sense for rdf-engine
                # deterministic. reduce blank nodes in output
                id = t.subject
                if deterministic_nested:
                    old = id
                    id = hash(ndt)
                    id = abs(id)
                    id = self.terms.nn(uri, id)
                    new = id
                else:
                    id = t.subject
                    old = new = id
                old2new[old] = new
                    
                yield T(id, trm.type,       trm.Statement)
                yield T(id, trm.subject,    ndt.subject)
                yield T(id, trm.predicate,  ndt.predicate)
                yield T(id, trm.object,     ndt.object)
                # meta       t.reifies
                #yield T(id, t.predicate,    t.object)
                # meta in below loop

        for t in str:
            if not isnested(t):
                if t.subject in old2new:
                    t = T(old2new[t.subject], t.predicate, t.object)
                if t.object in old2new:
                    t = T(t.subject, t.predicate, old2new[t.object])
                yield t
                
    # :man :hasSpouse :woman .
    # :id1 rdf:type rdf:Statement ;
        # rdf:subject :man ;
        # rdf:predicate :hasSpouse ;
        # rdf:object :woman ;
        # :startDate "2020-02-11"^^xsd:date .
    # <<:man :hasSpouse :woman>> :startDate "2020-02-11"^^xsd:date .
    class query:
        # https://www.ontotext.com/knowledgehub/fundamentals/what-is-rdf-star/
        class parts:
            rei = """
            ?reification a rdf:Statement .
            ?reification rdf:subject ?subject .
            ?reification rdf:predicate ?predicate .
            ?reification rdf:object ?object .
            """
            left =  "?reification ?p ?o."
            right = "?s ?p ?reification."
            reil = rei + left
            reir = rei + right
            union = f"""
            {rei}
            {{{left}}} union {{{right}}}
            """
        std2str = """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        CONSTRUCT {
            #<<?subject ?predicate ?object>> ?p ?o .
            #?s ?p <<?subject ?predicate ?object>> .
            ?reification rdf:reifies <<(?subject ?predicate ?object)>>. # tricky!!
            side
        } WHERE {
            part
            FILTER (?p NOT IN (rdf:subject, rdf:predicate, rdf:object) &&
            (?p != rdf:type && ?object != rdf:Statement))
        }
        """
        std2strl = std2str.replace('part', parts.reil).replace('side', parts.left)
        std2strr = std2str.replace('part', parts.reir).replace('side', parts.right)
        delrei = """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        DELETE where  { part }
        """
        delreil = delrei.replace('part', parts.reil)
        delreir = delrei.replace('part', parts.reir)
    def star(self, std: Iterable[Triple],) -> Iterable[Triple]:
        from pyoxigraph import Store, Quad
        s = Store()
        s.bulk_extend(Quad(*t) for t in std)
        yield from s.query( self.query.std2strl)
        yield from s.query( self.query.std2strr)
        s.update(           self.query.delreil)
        s.update(           self.query.delreir)
        #                             the 'regular' ones
        yield from s.query("construct {?s ?p ?o} where {?s ?p ?o.}")


reification = _reification()


from .db import Ingestable
from typing import Iterable
from pyoxigraph import Quad
def quads(i: Ingestable) -> Iterable[Quad]:
    from .db import ingest, Store
    yield from ingest(Store(), i)
