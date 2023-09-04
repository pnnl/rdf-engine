#https://github.com/RDFLib/OWL-RL/blob/master/owlrl/Closure.py
# the 'deepest' idea is:     Store(Graph) -f-> Store(Graph)
# gen rules...uggh tricky    Store(Graph) -f-> (Store(Graph) -f2-> Store(Graph)  )
# validation is 'adjacent'

from .abstract import abstract as b
import pyoxigraph as g
from typing import Iterable, Iterator
# meta/rdfstar inserstion somewhere TODO


def it():
    i = 0
    while True:
        yield i
        i += 1

#hashes = set()

def _normalize_h(triples: Iterable[g.Triple]) -> Iterable[g.Triple]:
    # is the skolemization ok?
    triples = tuple(triples, )
    if not triples: return ()
    def tohash():
        # hash the 'constants' to id the set of triples.
        # ...not the ones that can potentially change
        for s,p,o in triples:
            if isinstance(s, g.NamedNode):
                yield (s)
            if isinstance(p, g.NamedNode):
                yield (p)
            if isinstance(o, g.NamedNode):
                yield (o)
    from hashlib import md5 # md5 instead of __hash__ bc hash() changes bw program launches
    tohash = (str(t.value) for t in tohash())
    tohash = sorted(tohash)
    tohash = ''.join(t for t in tohash)
    tohash = tohash.encode()
    tohash = md5(tohash)
    tohash = tohash.hexdigest()


    url = f'http://deanon/{tohash}/'
    iz = iter(it())
    bnm = {} # bn -> iz
    # give a num to the blank nodes
    for s, _, o in triples:
        if isinstance(s, g.BlankNode): bnm[s] = next(iz)
        if isinstance(o, g.BlankNode): bnm[o] = next(iz)
        # NA really
        if isinstance(_, g.BlankNode): bnm[_] = next(iz)
    
    __ = []
    for spo in (triples):
        _ = map(lambda s: g.NamedNode(f'{url}{bnm[s]}') if s in bnm else s, spo  )
        _ = g.Triple(*_)
        __.append(_)
    __ = tuple(__) # immutable
    return __


global_counter = iter(it())
# this doesnt work. doesnt uniquify
def _normalize_c(triples: Iterable[g.Triple]) -> Iterable[g.Triple]:
    def url(): return f'http://deanon/{next(global_counter)}'
    __ = []
    for en, (s,p,o) in enumerate(triples):
        if isinstance(s, g.BlankNode) and isinstance(o, g.BlankNode):
            if s == o:
                s = o = g.NamedNode(f'{url()}')
            else:
                s = g.NamedNode(f'{url()}')
                o = g.NamedNode(f'{url()}')
        elif isinstance(s, g.BlankNode) and not isinstance(o, g.BlankNode):
            s = g.NamedNode(f'{url()}')
        elif not isinstance(s, g.BlankNode) and isinstance(o, g.BlankNode):
            o = g.NamedNode(f'{url()}')
        else: # no change
            pass
        _ = g.Triple(s,p,o)
        __.append(_)
    __ = tuple(__) # immutable
    return __


def normalize(triples: Iterable[g.Triple]) -> Iterable[g.Triple]:
    # anon nodes get a unique id
    # need to deanon nodes so that we dont get
    # new anon nondes every time data with anon nodes is read.
    _ = triples
    _ = _normalize_h(_)
    return _


class Triples(b.Data):

    def __init__(self, data: Iterable[g.Triple]=[]) -> None:
        _ = normalize(data)
        self._data = _
    
    def __hash__(self) -> int:
        return hash(frozenset(self))
    
    def __len__(self) -> int:
        i = 0
        for _ in self: i += 1
        return i

    def __iter__(self) -> Iterable[g.Triple]:
        yield from self._data

    def __add__(self, data: 'Triples' ) -> 'Triples':
        from itertools import chain 
        return Triples(chain(self._data, data._data))

    def insert(self, db: 'OxiGraph', graph=g.DefaultGraph()) -> None:
        from io import BytesIO
        fmt='application/n-triples'
        _ = BytesIO()
        g.serialize(self._data, _, fmt)
        _.seek(0)
        if len(_.getbuffer()):
            db._store.bulk_load(_, fmt, to_graph=graph)


class OxiGraph(b.DataBase):
    
    def __init__(self, db: g.Store=g.Store()) -> None:
        self._store = db
    
    def __iter__(self) -> Iterable[g.Quad]:
        yield from self._store

    def __len__(self):
        return len(self._store)
    
    def __hash__(self) -> int:
        # return len(self._store)#  not strictly correct
        # return _
        _ = self._store
        # pyoxigraph doesn't hash the contents
        _ = frozenset(_)
        _ = hash(_)
        return _

    # on the query
    #def query(self, query: 'SelectQuery') -> 'SelectQueryData':
    #    _ = query(self)
    #    return _

    def insert(self, data: b.Data) -> None:
        data.insert(self)


class SelectQueryData(b.Data):
    def __init__(self, result: g.QuerySolutions | Iterable[g.QuerySolution] ) -> None:
        self._result = result

    def __iter__(self):
        yield from self._result

    def __add__(self, data: 'SelectQueryData') -> 'SelectQueryData':
        from itertools import chain
        return SelectQueryData(chain(self._result, data._result))
    
    def insert(self, db: OxiGraph) -> None:
        raise NotImplementedError
    

class SelectQuery(b.Query):

    def __init__(self, query: str) -> None:
        assert('select' in query.lower()) # TODO: need to programmatically check this
        self._query = query

    def __add__(self, query: 'SelectQuery') -> 'SelectQuery':
        raise NotImplementedError

    def __str__(self) -> str:
        return str(self._query)

    def __call__(self, db: OxiGraph, **k) -> SelectQueryData:
        _ =  db._store.query(str(self),  **k)
        assert(isinstance(_, g.QuerySolutions))
        _ = SelectQueryData(_)
        return _


class ConstructQuery(b.Query):

    def __init__(self, query: str) -> None:
        assert('construct' in query.lower()) # TODO: need to programmatically check this
        self._query = query

    def __add__(self, query: 'ConstructQuery') -> 'ConstructQuery':
        raise NotImplementedError

    def __str__(self) -> str:
        return str(self._query)

    def __call__(self, db: OxiGraph, **k) -> Triples:
        _ =  db._store.query(str(self),  **k)
        assert(isinstance(_, g.QueryTriples))
        _ = Triples(_)
        return _


called = set() # [(rule, dbstate), ...]

class CachedRuleCall:

    def __call__(self, db: OxiGraph) -> Triples:
        #return self.do(db)  # disabling
        #h = hash((db)) # len(db) is risky
        h = len(db)
        call = (hash(self), h)
        if call in called:
            return Triples([]) # was already captured
        else:
            called.add(call)
            return self.do(db)

        
class Rule(CachedRuleCall, b.Rule):

    def __hash__(self) -> int:
        return hash(self.spec)

    def add_meta(self, data: Triples) -> Triples:
        def nested(data):
            _ = self.meta(data)
            ms = tuple(_)
            if ms:
                for t in data:
                    for m in ms:
                        yield g.Triple(t, g.NamedNode('http://meta'), m)
            yield from data
        _ = nested(data)
        _ = Triples(_)
        return _
    

class ConstructRule(Rule):

    def __init__(self, spec: ConstructQuery) -> None:
        self._spec = spec
    
    @property
    def spec(self) -> ConstructQuery:
        return self._spec

    def __add__(self, rule: 'Rule') -> 'Rules':
        return Rules([self, rule])
    
    def const_meta(self) -> Triples:
        # something that doesn't depend on the data
        return Triples([])

    def meta(self, data: Triples) -> Triples:
        # can add query str. or triples!
        # TODO
        return Triples([])

    def do(self, db: OxiGraph) -> Triples:
        _ = db._store.query(str(self.spec))
        assert(isinstance(_, g.QueryTriples))
        _ = Triples(_)
        _ = self.add_meta(_) 
        return _


from typing import Protocol, runtime_checkable
@runtime_checkable
class PyRuleCallable(Protocol):
    def __call__(self, db: OxiGraph) -> Triples:
        raise NotImplementedError
#Callable[[OxiGraph], Triples]

class PyRule(Rule):

    def __init__(self, spec: PyRuleCallable) -> None:
        self._spec = spec
    
    @property
    def spec(self) -> PyRuleCallable:
        return self._spec
    
    def meta(self, data: Triples) -> Triples:
        # can add query str. or triples!
        return Triples([])
    
    def __add__(self, rule: 'Rule') -> 'Rules':
        return Rules([self, rule])
    
    def do(self, db: OxiGraph) -> Triples:
        _ = self.spec(db)
        _ = Triples(_)
        _ = self.add_meta(_)
        return _


def _idf(db: OxiGraph): return Triples([])
idf: PyRuleCallable = _idf
NoEffect = PyRule(idf)



Spec = PyRuleCallable | ConstructQuery
RulesSpec = Iterable[ Spec ]
Rule = PyRule | ConstructRule
class Rules(b.Rules):

    def __init__(self, rules: Iterator[Rule] ) -> None:
        self.rules = rules
    
    @property
    def spec(self) -> RulesSpec:
        return list(r.spec for r in self.rules)
    
    
    def __iter__(self) -> Iterator[Rule]:
        for r in self.rules: yield r


    def meta(self, data: Triples) -> Triples:
        # get the meta from each
        _ = map(lambda cr: cr.meta(data)._data, self)
        from itertools import chain
        _ = chain.from_iterable(_)
        return _


    def __add__(self, rules: 'Rules') -> 'Rules':
        _ = list(self.rules)+list(rules.rules)
        return Rules(_)
    
    def __call__(self, db: OxiGraph) -> Triples:
        _ = map(lambda r: (r)(db)._data, self.rules)
        from itertools import chain
        _ = chain.from_iterable(_)
        _ = Triples(_)
        return _


class Validation(b.Validation):
    ...


class Result(b.Result):
    def __init__(self, db: OxiGraph, result: b.Success | b.Failure ) -> None:
        self._db = db
        self._res = result
    
    def __bool__(self) -> b.Success | b.Failure:
        return self._res
    
    @property
    def db(self) -> OxiGraph:
        return self._db
    
    def __call__(self) -> OxiGraph:
        return self.db


iz = iter(it())


import logging # :( i dont do module level imports
logger = logging.getLogger('engine')

class Engine(b.Engine): # rule app on Store

    def __init__(self, rules: Rules, db: OxiGraph, MAX_ITER=999,
                 log=True, print_log=True,
                 ) -> None:
        self._rules = rules
        self._db = db
        #db._store.dump(open('init.ttl', 'wb'), 'text/turtle')
        self.MAX_ITER = MAX_ITER
        
        # logging
        if log:
            from collections import defaultdict, namedtuple
            from types import SimpleNamespace as NS
            self.logging = NS(
                print = print_log,
                log = defaultdict(list),
                delta = namedtuple('delta', ['before', 'after'] )
            )
        

    @property
    def rules(self) -> Rules:
        return self._rules
    
    @property
    def db(self) -> OxiGraph:
        return self._db
    
    def validate(self) -> Result:
        raise NotImplementedError    
    
    def crank_once(self) -> OxiGraph:
        #_ = map(lambda r: r(self.db), self.rules)
        #from itertools import chain
        #_ = chain.from_iterable(_)
        #_: Iterable[g.Triple] = chain.from_iterable(_)
        # _.insert(self.db)
        for r in self.rules:
            before = len(self.db)
            _ = r(self.db)
            _.insert(self.db)

            if hasattr(self, 'logging'):
                delta = self.logging.delta(before, len(self.db))
                self.logging.log[r.spec].append(delta)
                if self.logging.print:
                    logger.info(
                        f"{repr(r)}: # triples before {delta.before }, after {delta.after }")
            
        return self.db

    def stop(self) -> bool:
        # could put validations here
        if len(self.db) == len(self.crank_once()):
            return True
        else:
            return False
    
    def __iter__(self) -> Iterable[OxiGraph]:
        MAX_ITER = self.MAX_ITER
        i = 0
        while (not self.stop()):
            if i > MAX_ITER:
                print('reached max iter')
                break
            yield self.db
            i += 1
        else: # for case when nothing needs to happen
            yield self.db

    def final(self) -> OxiGraph:
        from collections import deque
        def tail(n, iterable):
            "Return an iterator over the last n items"
            # tail(3, 'ABCDEFG') --> E F G
            return iter(deque(iterable, maxlen=n))
        _ = tail(1, self)
        _ = tuple(_)
        _ = _[0]
        return _
    
    def __call__(self) -> Result:
        db = self.final()
        return Result(db, True)


if __name__ == '__main__':
    # consume dir for data
    ...
