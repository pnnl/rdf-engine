![PyPI - Status](https://img.shields.io/pypi/v/rdfengine)

# RDF-Engine

## Why?

Motivation: This was developed as part of [BIM2RDF](https://github.com/PNNL/BIM2RDF)
where the conversion from BIM to RDF is framed as 'mapping rules'.

## How?

Rules are processes that generate triples.
They are simply applied until no _new_ triples are produced.
[Oxigraph](https://github.com/oxigraph/oxigraph) is used to store data.

## Features

* Handling of anonymous/blank nodes: they can be deanonimized
* Oxigraph can handle RDF-star data and querying


## Development Philosophy
* **KISS**: It should only address executing rules.
Therefore, the code is expected to be feature complete (without need for adding more 'features').
* **Minimal dependencies**: follows from above.