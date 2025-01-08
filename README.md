![PyPI - Status](https://img.shields.io/pypi/v/rdf-engine)


# RDF-Engine

## Why?

Motivation: This was developed as part of [BIM2RDF](https://github.com/PNNL/BIM2RDF)
where the conversion from BIM to RDF is framed as 'mapping rules'.

## How?

Rules are processes that generate triples.
They are simply applied until no _new_ triples are produced.
[Oxigraph](https://github.com/oxigraph/oxigraph) is used to store data.

A rule is defined as a function that takes an Oxigraph instance
and returns quads.


## Features

* Handling of anonymous/blank nodes: They can be deanonimized.
* Oxigraph can handle RDF-star data and querying.
However, if 

## Usage

The package can be obtained from [PyPi](https://pypi.org/project/rdf-engine/).
Note the CLI is optional: `rdf-engine[cli]`.

A 'program' is defined as a sequence of engine runs
initialized by a `db'.
Each engine run takes:
* (engine) `params`
* and a list of `rules`.
Each rule needs a `module`, `maker`, and `params`.
See program example in [test script](./test/program.yaml).


## Development Philosophy
* **KISS**: It should only address executing rules.
Therefore, the code is expected to be feature complete (without need for adding more 'features').
* **Minimal dependencies**: follows from above.
* **Explicit > Implicit**: Parameters should be specificed (explicitly).
