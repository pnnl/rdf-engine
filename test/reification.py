import marimo

__generated_with = "0.16.5"
app = marimo.App(width="columns", auto_download=["html"])


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _(mo):
    mo.md(r"""if you keep executing the below cell, you'll see randomness but not in the following cells.""")
    return


@app.cell
def _():
    import pyoxigraph as po
    _ = """
    prefix d: <urn:DATA:>
    prefix m: <urn:META:>

    #<<d:s d:p x:o>> m:p m:o .
    # https://github.com/oxigraph/oxigraph/issues/1286
    <<d:s d:p d:o>> m:p m:o .
    <<d:s d:p d:o>> m:p2 m:o2 .
    # not the same as
    #<<d:s d:p d:o>> m:p m:o; m:p2 m:o2 .

    #m:ss m:pp <<d:ss d:pp d:oo>>.
    #m:ss m:pp <<(d:ss d:pp d:oo)>>.  # not handled
    #<<(d:s d:p d:o)>> m:p m:o .  # not possible
    """
    nested = po.parse(_, format=po.RdfFormat.TURTLE)
    nested = list(nested)
    nested
    return (nested,)


@app.cell
def _(nested):
    import rdf_engine.data as d
    std = sorted(list(frozenset(d.reification.standard(nested))), key=lambda t: str(t) )
    std
    return d, std


@app.cell
def _(d, std):
    star = d.reification.star(std)
    star = list(frozenset(star))
    star
    return


if __name__ == "__main__":
    app.run()
