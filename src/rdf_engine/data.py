
class _reification:
    from typing import Iterable
    from pyoxigraph import Triple, NamedNode
    class terms:
        prefix = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
        terms = {'Statement', 'type',
                 'subject', 'predicate', 'object'}
        def __init__(self):
            for t in self.terms:
                setattr(self, t, self.nn(self.prefix, t))
        @staticmethod
        def nn(prefix, term):
            from pyoxigraph import NamedNode
            return NamedNode(f"{prefix}{term}")
    terms = terms()
    def standard(self, str: Iterable[Triple]) -> Iterable[Triple]:
        T = self.Triple
        trm = self.terms
        for t in str:
            if not isinstance(t.subject, T):
                yield t
            else: # nested data triple
                ndt = t.subject
                assert(not  isinstance(t.object,  T))
                #                                  make positive...
                id = self.terms.nn("urn:meta:id:", abs(hash(t))) 
                # ...probably wont make use of the number (specifically)
                yield T(id, trm.type,       trm.Statement)
                yield T(id, trm.subject,    ndt.subject)
                yield T(id, trm.predicate,  ndt.predicate)
                yield T(id, trm.object,     ndt.object)
                # meta
                yield T(id, t.predicate,    t.object)
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
            std = """
            ?reification a rdf:Statement .
            ?reification rdf:subject ?subject .
            ?reification rdf:predicate ?predicate .
            ?reification rdf:object ?object .
            ?reification ?p ?o .
            """
        std2str = """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        CONSTRUCT {
            <<?subject ?predicate ?object>> ?p ?o .
        } WHERE {
            parts.std
            FILTER (?p NOT IN (rdf:subject, rdf:predicate, rdf:object) &&
            (?p != rdf:type && ?object != rdf:Statement))
        }
        """
        std2str = std2str.replace('parts.std', parts.std)
        delstd = """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        DELETE where { parts.std }
        """
        delstd = delstd.replace('parts.std', parts.std)
    def star(self, std: Iterable[Triple]) -> Iterable[Triple]:
        from pyoxigraph import Store, Quad
        s = Store()
        s.bulk_extend(Quad(*t) for t in std)
        yield from s.query( self.query.std2str)
        s.update(           self.query.delstd)   # delete std
        #                             the 'regular' ones
        yield from s.query("construct {?s ?p ?o} where {?s ?p ?o.}")


reification = _reification()


from .db import Ingestable
from typing import Iterable
from pyoxigraph import Quad
def quads(i: Ingestable) -> Iterable[Quad]:
    from .db import ingest, Store
    yield from ingest(Store(), i)
