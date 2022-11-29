import os
from uuid import uuid4
import orjson
from urllib.parse import unquote
from pathlib import Path

from fastapi import FastAPI, Request, Query, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.gzip import GZipMiddleware

from lakota import Repo
from lakota.utils import logger
from numpy import asarray, char
from numpy.core.defchararray import find


lk_repo_url = os.environ.get("LAKOTA_REPO", ".lakota")
logger.setLevel("CRITICAL")
static_prefix = "static"
PAGE_LEN = 500_000
LRU_SIZE = 500 * 1024 * 1024 # 500MB
repo = Repo([
    f"memory://?lru_size={LRU_SIZE}",
    lk_repo_url,
])
here = Path(__file__).resolve().parent


title = "LK-web"
app = FastAPI(app_name=title)
templates = Jinja2Templates(directory=here / "template")
app.mount("/static", StaticFiles(directory=here / "static"), name="static")
app.add_middleware(GZipMiddleware, minimum_size=100_000)

uplot_options = {
    # 'title': '',
    # 'id': '',
    # 'class': '',
    "width": 900,
    "height": 300,
    "series": [
        {},
        {
            # initial toggled state (optional)
            "show": True,
            "spanGaps": False,
            # in-legend display
            "label": "Value1",
            # series style
            "stroke": "red",
            "width": 1,
            "fill": "rgba(255, 0, 0, 0.3)",
            "dash": [10, 5],
        },
    ],
}


class ORJSONResponse(JSONResponse):
    def render(self, content):
        return orjson.dumps(
            content, option=orjson.OPT_SERIALIZE_NUMPY | orjson.OPT_INDENT_2
        )


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    # logo = (static_path / 'jensen-sm.png').open('rb').read()
    # logo = "data:image/png;base64, " + b64encode(logo).decode()
    logo = ""
    return templates.TemplateResponse(
        "index.html",
        {
            "title": title,
            "b64logo": logo,
            "request": request,
        },
    )


@app.get("/favicon.ico")
async def favico():
    return ""  # TODO


@app.get("/search")
def search(request: Request, label: str = ""):
    patterns = label.split()
    all_labels = []
    for name in repo.ls():
        clct = repo / name
        labels = asarray(clct.ls(), dtype="U")
        for pattern in patterns:
            cond = find(char.lower(labels), pattern.lower()) != -1
            all_labels.extend((name, l) for l in labels[cond])

    return templates.TemplateResponse(
        "search-modal.html",
        {
            "labels": all_labels,
            "request": request,
        },
    )


@app.get("/series/{collection}/{series}")
def series(request: Request, collection: str, series: str):
    series = unquote(series).strip()
    collection = unquote(collection).strip()
    clct = repo / collection
    columns = []
    for name, info in clct.schema.columns.items():
        if name in clct.schema.idx:
            continue
        if info.codec.dt not in ("f8", "i8"):
            continue
        columns.append(name)

    return templates.TemplateResponse(
        "series.html",
        {
            "collection": collection,
            "series": series,
            "columns": columns,
            "request": request,
        },
    )


@app.get("/view/{collection}/{label}/{column}")
@app.get("/view/{collection}/{label}/{column}/page/{page}")
def view(
    request: Request,
    collection: str,
    label: str,
    column: str,
    page: int = Query(0),  ## TODO use explicit limit/offset
    view: str = Query(""),
    switch_view: str = Query(""),
    prev_next: str = Query(""),
    start: str = Query(None),
    stop: str = Query(None),
):
    collection = unquote(collection).strip()
    label = unquote(label).strip()
    column = unquote(column).strip()
    inputs = {}
    params = {
        "page": 0,
        "start": start,
        "stop": stop,
        "view": "table" if view == "table" else "graph"
    }

    if switch_view:
        params["view"] = switch_view
    if prev_next:
        page += 1 if prev_next == "next" else -1
        page = max(page, 0)
        params["page"] = page

    params = {k:v for k,v in params.items() if v is not None}
    path = f"{collection}/{label}/{column}"
    return templates.TemplateResponse(
        "view.html",
        {
            "path": path,
            "params": params,
            "page_len": PAGE_LEN,
            "collection": collection,
            "label": label,
            "column": column,
            "inputs": inputs,
            "show_filters": bool(start or stop),
            "request": request,
            "graph_id": "graph-" + uuid4().hex[:8],
        },
    )

@app.get("/read/{collection}/{label}/{column}")
@app.get("/read/{collection}/{label}/{column}.{ext}")
def read(
    request: Request,
    response: Response,
    collection: str,
    label: str,
    column: str,
    ext: str = "json",
    page: int = Query(0),
    start: str = Query(None),
    stop: str = Query(None),

):
    collection = unquote(collection).strip()
    label = unquote(label).strip()
    column = unquote(column).strip()
    series = repo / collection / label

    # find time dimension
    tdim = None
    for name, coldef in series.schema.idx.items():
        if coldef.codec.dt == "datetime64[s]":
            tdim = name
            break
    else:
        # No time dimension found
        return

    # Query series
    extra_cols = tuple()  # tuple(col for col, value in params.items() if value)
    cols = (tdim, column) + extra_cols
    cols = (tdim, column) + extra_cols
    frm = series.frame(
        start=start, stop=stop,
        limit=PAGE_LEN, offset=page * PAGE_LEN,
        select=cols)

    if ext == "html":
        response = templates.TemplateResponse(
            "table.html",
            {
                "request": request,
                "columns": cols,
                "frame": frm,
        })
    else:
        # Aggregate on time dimension
        if len(series.schema.idx) > 1:
            dt = frm[column].dtype
            agg_col = f"(last self.{column})"
            frm = frm.reduce(tdim, agg_col)
            data = [frm[tdim].astype(int), frm[agg_col].astype(dt)]
        else:
            data = [frm[tdim].astype(int), frm[column]]

        # Build response
        data = {"data": data}
        if ext == 'graph':
            data["options"] = uplot_options
        response = ORJSONResponse(data)

    return response
