import logging # :( i dont do module level imports
logger = logging.getLogger(__name__)
del logging


class Engine:
    from .rules import Rule
    from typing import Iterable, Literal
    class DeanonPrefix(str): ...
    from pyoxigraph import Store
    from inspect import signature
    from .canon import quads
    def __init__(self,
        rules: Iterable[Rule] = [], *,
        db: Store=Store(),
            MAX_NCYCLES: int=99,
        # safe settings to avoid inf cycling
        # but reduces performance
        derand: Literal['canonicalize'] | DeanonPrefix | Literal[False]  = signature(quads.deanon).parameters['uri'].default,
        # typically expecting the engine to be used in a stand-alone program
        # so it helps to have log_print.
            log: bool=True, log_print: bool=False,
            debug: bool = False,
        ) -> None:
        self.rules = list(rules)
        self.db = db

        self.MAX_NCYCLES = MAX_NCYCLES

        # derand --> (deanon, deanon_uri, canon)
        canon = 'canonicalize'
        if derand not in {canon, False}:
            assert(isinstance(derand, str))
        
        if (derand == canon) or (isinstance(derand, str)):
            self.canon = True
        else:
            self.canon = False
        
        if derand == canon:
            self.deanon = False
        elif derand == False:
            self.deanon = False
        else:
            assert(isinstance(derand, str))
            self.deanon = True
            self.deanon_uri = derand

        self.i = 0
        
        # logging
        if log:
            from collections import namedtuple
            from types import SimpleNamespace as NS
            self.logging = NS(
                print = log_print,
                log = [],
                state = namedtuple('state', ['cycle', 'stored', 'rule', 'new', 'time'] ))
        self.debug = debug

    # TODO: make a method for applying one rule
    def run1(self) -> Store:
        if hasattr(self, 'logging'):
            if self.logging.print:
                line = '-'*10
                logger.info(f"CYCLE {self.i} {line}")

        def process(qs):
            _ = qs
            if self.canon:
                # have to keep it per rule firing. bc too much data potentially.
                from .data import quads
                _ = quads(_)
                from .canon import quads
                _ = quads(_)
                if self.deanon:
                    _ = quads.deanon(_, uri=self.deanon_uri)
            yield from _

        from .db import ingest
        for ir, r in enumerate(self.rules): # TODO: could be parallelized:
            # before
            if hasattr(self, 'logging'):
                if self.logging.print:
                    logger.info(f"  {repr(r)}")
                from time import monotonic
                start_time = monotonic()
                nquads = len(self.db)
            # do
            _ = r(self.db)
            _ = process(_)
            if self.debug:
                if self.logging.print:
                    _ = tuple(_)
                    for q in _: logger.debug(f"{q}")
            ingest(self.db, _, flush=True)
            # after
            if hasattr(self, 'logging'):
                self.logging.log.append(self.logging.state(
                    cycle=self.i,
                    stored=nquads,
                    rule=r,
                    new=len(self.db)-nquads,
                    time=float('{0:.2f}'.format(monotonic()-start_time))))
                if self.logging.print:
                    s = self.logging.log[-1]
                    width = 6
                    qi = 'quads in'
                    logger.info('   '+f"{s.new:<{width}} {qi if ir==0 else ' '*len(qi)} {s.time}s"  )
                    del s, width, qi
            # so if a rule returns a string,
            # it /could/ go in fast in the case of no processing (canon/deanon)
            del _
        self.i += 1
        self.db.flush()
        self.db.optimize()
        if hasattr(self, 'logging'):
            if self.logging.print:
                logger.info(f'db has {len(self.db)} quads')
        return self.db

    def stop(self) -> bool:
        if self.MAX_NCYCLES <= 0:
           return False
        # could put validations here
        if len(self.db) == len(self.run1()):
            return True
        else:
            return False
    
    def __iter__(self) -> Iterable[Store]:
        while (not self.stop()):
            if self.i >= self.MAX_NCYCLES:
                from warnings import warn
                warn('reached max cycles')
                break
            yield self.db
        else: # for case when nothing needs to happen
            yield self.db

    def run(self) -> Store:
        for _ in self: continue
        return self.db
    __call__ = run

