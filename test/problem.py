c1 = """PREFIX ex: <http://example.com/>
CONSTRUCT {
    _:ci ex:city ?city.
}
WHERE {
  VALUES (?city) { ("New York") ("London") }
}"""
c2 = """
PREFIX ex: <http://example.com/>
CONSTRUCT {
    ?ci ex:city ?city. # bad w/ 'just' canonicalize. ok with urn:deanon:.
    ?ci ex:city2 ?city.
}
WHERE {
    ?ci ex:city ?city.
}
"""
from pyoxigraph import Store, Quad
def r1(db: Store,): yield from (Quad(*t) for t in db.query(c1))
def r2(db: Store,): yield from (Quad(*t) for t in db.query(c2))

from rdf_engine import Engine, logger
import logging
logger.setLevel(logging.DEBUG)
logging.basicConfig()#force=True) # force removes other loggers that got picked up.
e = Engine([r1,r2],derand='urn:deanon:', log_print=True)  # ok always
#e = Engine([r1,r2],derand='canonicalize', MAX_NCYCLES=3, log_print=True, debug=False)  #  blows up.
e.run()
