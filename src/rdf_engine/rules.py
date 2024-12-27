from typing import Callable
from pyoxigraph import Store
from .db import Ingestable
Rule = Callable[[Store], Ingestable]

# it's possible to come up with an abstraction for a Rule
# but decided not to (here at least).

# an engine run (itself) could be taken as a 'rule'.