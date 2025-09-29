import marimo

__generated_with = "0.16.3"
app = marimo.App(width="columns", auto_download=["html"])


@app.cell
def _():
    import pyoxigraph as po
    _ = """
    prefix d: <urn:DATA:>
    prefix m: <urn:META:>

    #<<d:s d:p x:o>> m:p m:o .
    # https://github.com/oxigraph/oxigraph/issues/1286
    #<<d:s d:p d:o>> m:p m:o .
    #<<d:s d:p d:o>> m:p2 m:o2 .
    # not the same as
    #<<d:s d:p d:o>> m:p m:o; m:p2 m:o2 .

    m:pp m:oo <<d:ss d:pp d:oo>>.
    """
    nested = po.parse(_, format=po.RdfFormat.TURTLE)
    nested = list(nested)
    nested
    return (nested,)


@app.cell
def _(nested):
    import rdf_engine.data as d
    std = list(d.reification.standard(nested))
    std
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
