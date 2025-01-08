import logging # :( i dont do module level imports
logger = logging.getLogger('engine')


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
        derand: Literal['canonicalize'] | DeanonPrefix | False  = signature(quads.deanon).parameters['uri'].default ,
        # typically expecting the engine to be used in a stand-alone program
        # so it helps to have log_print.
            log: bool=True, log_print: bool=False,
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
            from collections import defaultdict, namedtuple
            from types import SimpleNamespace as NS
            self.logging = NS(
                print = log_print,
                log = defaultdict(list),
                delta = namedtuple('delta', ['before', 'after'] ))

    # TODO: make a method for applying one rule
    def run1(self) -> Store:
        if hasattr(self, 'logging'):
            if self.logging.print:
                line = '-'*10
                logger.info(f"CYCLE {self.i} {line}")

        for r in self.rules: # TODO: could be parallelized
            # before
            if hasattr(self, 'logging'):
                before = len(self.db)
                if self.logging.print:
                    logger.info(f"{repr(r)}")
                from time import monotonic
                start_time = monotonic()

            # do
            _ = r(self.db)
            if self.canon:
                from .data import quads
                _ = quads(_)
                from .canon import quads
                _ = quads(_)
                if self.deanon:
                    _ = quads.deanon(_, uri=self.deanon_uri)
            from .db import ingest
            # so if a rule returns a string,
            # it /could/ go in fast in the case of no processing (canon/deanon)
            ingest(self.db, _, flush=True)
            del _

            # after
            if hasattr(self, 'logging'):
                delta = self.logging.delta(before, len(self.db))
                self.logging.log[r].append(delta)
                if self.logging.print:
                    logger.info(f"# triples before {delta.before }, after {delta.after } => {delta.after-delta.before}.")
                    logger.info(f"took {'{0:.2f}'.format(monotonic()-start_time)} seconds")
        
        self.i += 1
        self.db.flush()
        self.db.optimize()
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
                if hasattr(self, 'logging'):
                    if self.logging.print:
                        logger.warning('reached max cycles')
                break
            yield self.db
        else: # for case when nothing needs to happen
            yield self.db

    def run(self) -> Store:
        for _ in self: continue
        return self.db
    __call__ = run

