from typing import Iterable
from pyoxigraph import Quad, Store
Ingestable = bytes|str|Iterable[Quad]
def ingest(s: Store, d:  Ingestable, *, flush=True):
    if isinstance(d, (str,bytes)):
        # assume ttl
        from pyoxigraph import RdfFormat
        s.bulk_load(d, format=RdfFormat.TURTLE)
    else:
        # assume iterable[quand]
        s.bulk_extend(d)
    
    if flush: s.flush()
    return s
