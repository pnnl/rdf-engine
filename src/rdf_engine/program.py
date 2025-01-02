"""
takes a program spec for execution
"""

from typing import Callable
from pathlib import Path

def chkdct(d: dict, t: dict[str, type]):
    for tn,tt in t.items():
        try:
            if not isinstance(d[tn], tt):
                raise ValueError(f'{tn} not of type {tt}')
        except TypeError:
            # some complicated thing not supported by isinstance
            ...


class Rule:
    from .rules import Rule
    @classmethod
    def mk(cls, i: dict) -> Rule:
        assert(isinstance(i, dict))
        if '.py' in i['module']:
            m = Path(i['module'])
            assert(m.exists())
            mod = {}
            _ = exec(open(m).read(), mod)
        else:
            from importlib import import_module
            mod = import_module(i['module']).__dict__
        maker = mod[i['maker']]
        assert(isinstance(maker, Callable))
        params = i['params'] if 'params' in i else {}
        return maker(**params)

#from .engine import Engine
# OO sucks
#class Engine(Engine):  # ❌
class Engine:           # ✔️
    from inspect import signature as sig
    from .engine import Engine
    args = {p:v.annotation
                for p,v in sig(Engine.__init__).parameters.items()
        if p not in {'self', }}
        
    @classmethod
    def mk(cls, i: dict):
        assert(isinstance(i, dict))
        args = cls.args
        if i['deanon'] == False: args.pop('deanon_uri')
        chkdct(i,  args)
        from .engine import Engine
        cls.log(i['log_print'])
        _ = Engine(**i)
        return _
    
    def log(self, enable=True):
        from . import logger
        import logging
        logging.basicConfig(force=True) # force removes other loggers that got picked up.
        logger.setLevel(logging.INFO)
        if not enable: logger.disabled = True


from dataclasses import dataclass
@dataclass
class Execution:
    params: dict
    rules: list[Rule.Rule]

    from typing import Self
    @classmethod
    def mk(cls, i: dict) -> Self:
        assert(isinstance(i, dict))
        rules = i['rules']
        assert(isinstance(rules, list))
        rules = [Rule.mk(r) for r in rules]
        return cls(
            params = i['params'],
            rules = rules
            )
    
    from pyoxigraph import Store
    def engine(self, db: Store):
        _ = {**self.params, 'rules': self.rules, 'db': db}
        _ = Engine.mk(_)
        return _


@dataclass
class Program:
    from pyoxigraph import Store
    db:     Path | Store | None
    execs:  list[Execution]

    @classmethod
    def mkdb(cls, i: Store | Path | str | None) -> Store:
        if i is None:
            return cls.Store()
        elif isinstance(i, Path):
            return cls.Store(i)
        elif isinstance(i, str):
            i = Path(i)
            return cls.Store(i)
        else:
            assert(isinstance(i, cls.Store))
            return i
    
    from typing import Self
    @classmethod
    def mk(cls, i: dict) -> Self:
        # db = i['db'] if 'db' in i else None
        db = cls.mkdb(i['db'])
        execs = [Execution.mk(e) for e in i['execs']]
        return cls(
            db = db,
            execs = execs
        )

    from typing import Self
    @classmethod
    def parse(self, i: str) -> Self:
        from pyyaml import safe_load

    def run(self) -> Store:
        for e in self.execs:
            e = e.engine(self.db)
            db = e.run()
        return db
        
    __call__ = run

