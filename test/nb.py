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
    `<<data.subject data.predicate data.object>> meta.predicate meta.object`.
          
    In order to derandomize, nested triples have to be converted to 'normal' triples
    in order to make use of canonicalization algorithms (that work on 'normal' triples).
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
    <<_:man3 d:hasSpouse _:woman3>> m:          _:m .
    # 'regular' triples.
    d:man1 d:hasSpouse d:woman1.
    d:man2 d:hasSpouse d:woman2.
    # should have same ids as the nested triples after derandomization
    _:man3 d:hasSpouse _:woman3.
    _:m    m:objectof   _:man3.
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
    _ = list(reification.standard(star))
    import rdflib as rl
    rg = rl.Graph()
    import rdf_engine.conversions as rc
    for t in rc.oxigraph.rdflib(_): rg.add(t)
    ttlstd =rg.serialize()
    ttlstd

@app.cell
def _(mo):
    mo.md("""
    To test invariance of the blank nodes,
    we'll try to invert the 'normal' triples back to rdf-star twice.
    
    inverse 1""")
@app.cell
def _(ttlstd, og, reification, ):
    def inv(og, ttlstd, reification):
        _ = og.parse(ttlstd, format=og.RdfFormat.TURTLE)
        _ = reification.star(q.triple for q in _)
        _ = og.serialize(_,  format=og.RdfFormat.TURTLE)
        _ = _.decode()
        return _
    inv(og, ttlstd, reification)
@app.cell
def _(mo):
    mo.md("""inverse 2: the id corresponding to `_:m` changes (though other blank nodes preserved). 😞 """)
@app.cell
def _(inv, ttlstd, og, reification,):
    inv(og, ttlstd, reification)
    
@app.cell
def _(mo):
    mo.md("""inverse 1 after canonicalization""")
@app.cell
def _(og, ttlstar):
    def canon(og, ttlstar):
        _ = og.parse(ttlstar, format=og.RdfFormat.TURTLE)
        import rdf_engine.canon as cn
        _ = cn.quads(_)
        _ = og.serialize(_,  format=og.RdfFormat.TURTLE)
        _ = _.decode()
        return _
    canon(og, ttlstar)
@app.cell
def _(mo):
    mo.md("""inverse 2 after canonicalization should mainain the ids 🙂 """)
@app.cell
def _(canon, og, ttlstar):
    canon(og, ttlstar)

if __name__ == "__main__":
    app.run()
