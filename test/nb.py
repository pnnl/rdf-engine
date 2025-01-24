# three languages in this notebook lol
import marimo
__generated_with = "0.10.16"
app = marimo.App(width="full")

@app.cell
def intro():
    import marimo as mo
    mo.md("""
    The most challenging part of the rules engine is handling of blank nodes.
    If a rule produces essentially the same data but with different blank nodes
    the engine will not stop (until it reaches a set maximum number of iterations).
    Furthermore, there can be blank nodes in nested triples.
    
    Consulting the excellent [GraphDB article on RDF-star](https://www.ontotext.com/knowledgehub/fundamentals/what-is-rdf-star/),
    it will be assumed that nested triples will be like
    `<data.subject data.predicate data.object> meta.predicate meta.object`.
          
    In order to derandomize, nested triples have to be converted to 'normal' triples
    in order to make use of canonicalization algorithms (that work on triples).
    Example:
    """)

@app.cell
def example():
    ttlstar = """
    prefix d: <urn:example:data:>
    prefix m: <urn:example:meta:>
    prefix xsd: <http://www.w3.org/2001/XMLSchema#>

    <<d:man1 d:hasSpouse d:woman1>> m:startDate "2011-01-11"^^xsd:date .
    <<d:man2 d:hasSpouse d:woman2>> m:startDate "2022-02-22"^^xsd:date .
    <<_:man3 d:hasSpouse _:woman3>> m:startDate _:date3 .
    """
    ttlstar
@app.cell
def _(mo):
    mo.md("""becomes""")


@app.cell
def _(ttlstar):
    import pyoxigraph as og
    star = og.parse(ttlstar, format=og.RdfFormat.TURTLE)
    star = list(q.triple for q in star)
    from rdf_engine.data import reification
    _ = reification.standard(star)
    import rdflib as rl
    rg = rl.Graph()
    import rdf_engine.conversions as rc
    for t in rc.oxigraph.rdflib(_): rg.add(t)
    ttlstd =rg.serialize()
    ttlstd

@app.cell
def _(mo):
    mo.md("""inverse""")
@app.cell
def _(ttlstd, og, reification, ):
    _ = og.parse(ttlstd, format=og.RdfFormat.TURTLE)
    _ = reification.star(q.triple for q in _)
    _ = og.serialize(_,  format=og.RdfFormat.TURTLE)
    _.decode()
    
    

if __name__ == "__main__":
    app.run()
