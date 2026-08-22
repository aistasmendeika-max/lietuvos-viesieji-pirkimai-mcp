import asyncio
import html
import json
import os
from typing import Any

import httpx
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse

load_dotenv()

mcp = FastMCP(
    "Lietuvos viešieji pirkimai",
    transport_security=TransportSecuritySettings(
        allowed_hosts=[
            "lietuvos-viesieji-pirkimai-mcp.onrender.com",
            "lietuvos-viesieji-pirkimai-mcp.onrender.com:*",
        ],
        allowed_origins=[
            "https://lietuvos-viesieji-pirkimai-mcp.onrender.com",
        ],
    ),
)

VPT_API_URL = os.getenv(
    "VPT_API_URL",
    "https://viesiejipirkimai.lt/epps-integration/api/cft-details-export",
)
VPT_PAGE_SIZE = 20
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "90"))

DEFAULT_BUYER = "Kretingos rajono savivaldybės administracija"
DEFAULT_KEYWORDS = [
    "Rudkasa",
    "Vakarų verslas",
    "sniego",
    "sniego išvežimas",
    "žiemos priežiūra",
]

# Kiek CVP IS puslapių tikrinti vienu paspaudimu.
BATCH_PAGES = int(os.getenv("BATCH_PAGES", "25"))
SEARCH_CONCURRENCY = int(os.getenv("SEARCH_CONCURRENCY", "5"))


def _api_key() -> str:
    key = os.getenv("VPT_API_KEY", "").strip()
    if not key:
        raise RuntimeError("Nenustatytas VPT_API_KEY Render → Environment.")
    return key


def _timeout() -> httpx.Timeout:
    return httpx.Timeout(connect=20.0, read=HTTP_TIMEOUT, write=30.0, pool=20.0)


def _json_records(payload: Any) -> list[Any]:
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, dict):
        return [payload]

    for key in (
        "content", "items", "results", "data", "records",
        "procurements", "cfts", "result"
    ):
        value = payload.get(key)
        if isinstance(value, list):
            return value
        if isinstance(value, dict):
            nested = _json_records(value)
            if nested:
                return nested

    return [payload]


def _text(value: Any) -> str:
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, default=str)


def _contains(value: Any, needle: str) -> bool:
    return needle.casefold() in _text(value).casefold()


def _matched_keywords(value: Any, keywords: list[str]) -> list[str]:
    haystack = _text(value).casefold()
    return [kw for kw in keywords if kw.casefold() in haystack]


def _keyword_list(raw: str) -> list[str]:
    values = [x.strip() for x in raw.replace(",", ";").split(";") if x.strip()]
    return values or DEFAULT_KEYWORDS


async def _fetch_page(page: int) -> Any:
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "apiKey": _api_key(),
        "User-Agent": "lietuvos-viesieji-pirkimai/7.0",
    }
    body = {"pageSize": VPT_PAGE_SIZE, "pageNum": page}

    async with httpx.AsyncClient(
        timeout=_timeout(),
        follow_redirects=True,
        http2=False,
    ) as client:
        response = await client.post(VPT_API_URL, headers=headers, json=body)

    if response.status_code >= 400:
        raise RuntimeError(
            f"CVP IS API HTTP {response.status_code}: {response.text[:800]}"
        )

    return response.json()


async def _scan_batch(
    buyer: str,
    keywords: list[str],
    start_page: int,
    pages: int,
    limit: int,
) -> dict[str, Any]:
    semaphore = asyncio.Semaphore(max(1, SEARCH_CONCURRENCY))

    async def one(page: int):
        async with semaphore:
            try:
                return page, await _fetch_page(page), None
            except Exception as exc:
                return page, None, f"{type(exc).__name__}: {exc}"

    page_numbers = list(range(start_page, start_page + pages))
    batch = await asyncio.gather(*(one(p) for p in page_numbers))

    results = []
    scanned = 0
    pages_ok = 0
    errors = []

    for page_num, payload, error in sorted(batch, key=lambda x: x[0]):
        if error:
            errors.append(f"p. {page_num}: {error}")
            continue

        pages_ok += 1
        records = _json_records(payload)

        for record in records:
            scanned += 1

            if buyer and not _contains(record, buyer):
                continue

            matched = _matched_keywords(record, keywords)

            results.append(
                {
                    "source": "Nauja CVP IS API",
                    "page": page_num,
                    "matched_keywords": matched,
                    "data": record,
                }
            )

            if len(results) >= limit:
                break

        if len(results) >= limit:
            break

    return {
        "buyer": buyer,
        "keywords": keywords,
        "start_page": start_page,
        "pages_requested": pages,
        "pages_ok": pages_ok,
        "records_scanned": scanned,
        "matches": len(results),
        "items": results,
        "next_page": start_page + pages,
        "errors": errors[:10],
    }


@mcp.tool()
async def search_procurements_batch(
    buyer: str = DEFAULT_BUYER,
    keywords: str = "Rudkasa; Vakarų verslas; sniego; sniego išvežimas; žiemos priežiūra",
    start_page: int = 1,
    pages: int = BATCH_PAGES,
    limit: int = 50,
) -> str:
    """
    Greita dalinė paieška. Tikrina nurodytą CVP IS puslapių paketą ir
    grąžina next_page reikšmę kitam paieškos etapui.
    """
    result = await _scan_batch(
        buyer.strip() or DEFAULT_BUYER,
        _keyword_list(keywords),
        max(1, int(start_page)),
        max(1, min(int(pages), 50)),
        max(1, min(int(limit), 200)),
    )
    return json.dumps(result, ensure_ascii=False, indent=2, default=str)


def _page(body: str) -> str:
    return f"""<!doctype html>
<html lang="lt"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Lietuvos viešųjų pirkimų paieška</title>
<style>
body{{font-family:Arial,sans-serif;margin:0;background:#f4f6f8;color:#17212b}}
main{{max-width:1180px;margin:36px auto;padding:0 20px}}
.card{{background:#fff;border-radius:14px;padding:24px;box-shadow:0 3px 16px rgba(0,0,0,.08);margin-bottom:20px}}
h1{{margin-top:0}}
form{{display:grid;grid-template-columns:1fr 1fr 120px;gap:10px}}
input,textarea,button{{font:inherit;padding:12px;border-radius:9px;border:1px solid #bcc5cf}}
textarea{{min-height:48px;resize:vertical}}
button{{background:#111827;color:#fff;cursor:pointer}}
button.secondary{{background:#334155}}
small{{color:#5b6673;display:block;margin-top:8px}}
pre{{white-space:pre-wrap;word-break:break-word;background:#f8fafc;padding:14px;border-radius:8px;overflow:auto}}
.result{{border-top:1px solid #e5e7eb;padding:16px 0}}
.badge{{display:inline-block;padding:5px 9px;background:#eef2ff;border-radius:999px;margin:3px}}
.hit{{display:inline-block;padding:4px 8px;background:#dcfce7;border-radius:999px;margin:3px}}
.warn{{background:#fff7ed;padding:12px;border-radius:8px}}
.actions{{display:flex;gap:10px;flex-wrap:wrap;margin-top:16px}}
a.button{{display:inline-block;text-decoration:none;background:#334155;color:white;padding:11px 14px;border-radius:9px}}
@media(max-width:800px){{form{{grid-template-columns:1fr}}}}
</style></head><body><main>{body}</main></body></html>"""


@mcp.custom_route("/", methods=["GET"])
async def web_search(request: Request) -> HTMLResponse:
    buyer = (request.query_params.get("buyer") or DEFAULT_BUYER).strip()
    raw_keywords = (
        request.query_params.get("keywords")
        or "Rudkasa; Vakarų verslas; sniego; sniego išvežimas; žiemos priežiūra"
    )
    keywords = _keyword_list(raw_keywords)

    try:
        start_page = max(1, int(request.query_params.get("start_page", "1")))
    except ValueError:
        start_page = 1

    try:
        limit = max(1, min(int(request.query_params.get("limit", "50")), 200))
    except ValueError:
        limit = 50

    run_search = request.query_params.get("run") == "1"

    form = f"""
<div class="card">
<h1>Lietuvos viešųjų pirkimų paieška</h1>
<p>Greita dalinė paieška pagal perkančiąją organizaciją. Tikrinama po {BATCH_PAGES} CVP IS puslapius vienu etapu.</p>
<form method="get" action="/">
<input name="buyer" value="{html.escape(buyer)}" placeholder="Perkančioji organizacija">
<textarea name="keywords" placeholder="Tiekėjai ir raktažodžiai">{html.escape('; '.join(keywords))}</textarea>
<input type="number" name="limit" min="1" max="200" value="{limit}" title="Maks. rezultatų">
<input type="hidden" name="start_page" value="1">
<input type="hidden" name="run" value="1">
<button type="submit">Ieškoti</button>
</form>
<small>Paieška nebevyksta per šimtus puslapių vienu metu, todėl Render neturėtų grąžinti 502 dėl per ilgos užklausos.</small>
</div>"""

    if not run_search:
        return HTMLResponse(_page(form))

    try:
        result = await _scan_batch(
            buyer,
            keywords,
            start_page,
            BATCH_PAGES,
            limit,
        )
    except Exception as exc:
        return HTMLResponse(
            _page(
                form
                + f'<div class="card"><h2>Paieškos klaida</h2>'
                + f'<div class="warn">{html.escape(type(exc).__name__ + ": " + str(exc))}</div></div>'
            ),
            status_code=500,
        )

    rows = []
    for i, item in enumerate(result["items"], 1):
        matched = item.get("matched_keywords", [])
        body = f"<p><strong>CVP IS puslapis:</strong> {item.get('page')}</p>"

        if matched:
            body += "<p><strong>Sutapo:</strong> " + "".join(
                f'<span class="hit">{html.escape(str(x))}</span>'
                for x in matched
            ) + "</p>"

        body += "<pre>" + html.escape(
            json.dumps(item["data"], ensure_ascii=False, indent=2, default=str)
        ) + "</pre>"

        rows.append(f'<div class="result"><h3>Rezultatas {i}</h3>{body}</div>')

    if not rows:
        rows.append(
            "<p>Šiame puslapių pakete pasirinktos organizacijos pirkimų nerasta.</p>"
        )

    error_html = ""
    if result["errors"]:
        error_html = (
            '<div class="warn"><strong>Dalis puslapių neatsakė:</strong><br>'
            + "<br>".join(html.escape(x) for x in result["errors"])
            + "</div>"
        )

    next_url = (
        "/?"
        + f"buyer={html.escape(buyer, quote=True)}"
        + f"&keywords={html.escape('; '.join(keywords), quote=True)}"
        + f"&limit={limit}"
        + f"&start_page={result['next_page']}"
        + "&run=1"
    )

    summary = f"""
<div class="card">
<h2>Rezultatai</h2>
<p><strong>Perkančioji organizacija:</strong> {html.escape(buyer)}</p>
<p>
<span class="badge">Tikrinami puslapiai: {start_page}–{start_page + BATCH_PAGES - 1}</span>
<span class="badge">Sėkmingai patikrinta: {result["pages_ok"]}</span>
<span class="badge">Įrašų patikrinta: {result["records_scanned"]}</span>
<span class="badge">Rasta: {result["matches"]}</span>
</p>
{error_html}
{''.join(rows)}
<div class="actions">
<a class="button" href="{next_url}">Ieškoti toliau nuo {result["next_page"]} psl.</a>
<a class="button" href="/">Nauja paieška</a>
</div>
</div>"""

    return HTMLResponse(_page(form + summary))


@mcp.custom_route("/api/search", methods=["GET"])
async def api_search(request: Request) -> JSONResponse:
    buyer = (request.query_params.get("buyer") or DEFAULT_BUYER).strip()
    keywords = _keyword_list(request.query_params.get("keywords") or "")

    try:
        start_page = max(1, int(request.query_params.get("start_page", "1")))
    except ValueError:
        start_page = 1

    try:
        pages = max(1, min(int(request.query_params.get("pages", str(BATCH_PAGES))), 50))
    except ValueError:
        pages = BATCH_PAGES

    try:
        limit = max(1, min(int(request.query_params.get("limit", "50")), 200))
    except ValueError:
        limit = 50

    try:
        return JSONResponse(
            await _scan_batch(buyer, keywords, start_page, pages, limit)
        )
    except Exception as exc:
        return JSONResponse(
            {
                "error": type(exc).__name__,
                "message": str(exc),
                "detail": repr(exc),
            },
            status_code=500,
        )


@mcp.custom_route("/health", methods=["GET"])
async def health(_: Request) -> HTMLResponse:
    return HTMLResponse("OK")


if __name__ == "__main__":
    mcp.settings.host = "0.0.0.0"
    mcp.settings.port = int(os.environ.get("PORT", "10000"))
    mcp.run(transport="streamable-http")
