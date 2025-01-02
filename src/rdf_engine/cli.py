
# arg parsing
from sys import argv
if not len(argv)==2:
    raise ValueError('error: give spec file')
spec = argv[1]

from pathlib import Path
spec = Path(spec)
assert(spec.exists())
spec = (spec.read_text())
from .program import Program
p = Program.parse(spec)
p.run()

exit(0)
