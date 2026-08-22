import asyncio
import html
import json
import os
import re
from typing import Any
from urllib.parse import urlencode

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
CVPP_BASE_URL = "https://cvpp.eviesiejipirkimai.lt/"
VPT_PAGE_SIZE = 20
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "120"))
MAX_NEW_PAGES = int(os.getenv("MAX_NEW_PAGES", "250"))
MAX_CVPP_RESULT_PAGES = int(os.getenv("MAX_CVPP_RESULT_PAGES", "20"))
SEARCH_CONCURRENCY = int(os.getenv("SEARCH_CONCURRENCY", "5"))


def _api_key() -> str:
    key = os.getenv("VPT_API_KEY", "").strip()
    if not key:
        raise RuntimeError("Nenustatytas VPT_API_KEY Render → Environment.")
    return key


def _timeout() -> httpx.Timeout:
    return httpx.Timeout(connect=20.0, read=HTTP_TIMEOUT, write=30.0, pool=20.0)


def _strip_tags(text: str) -> str:
    text = re.sub(r"(?is)<script.*?>.*?</script>", " ", text)
    text = re.sub(r"(?is)<style.*?>.*?</style>", " ", text)
    text = re.sub(r"(?s)<[^>]+>", " ", text)
    text = html.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def _json_records(payload: Any) -> list[Any]:
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, dict):
        return [payload]

    for key in ("content", "items", "results", "data", "records", "procurements", "cfts", "result"):
        value = payload.get(key)
        if isinstance(value, list):
            return value
        if isinstance(value, dict):
            nested = _json_records(value)
            if nested:
                return nested

    return [payload]


def _matches_text(value: Any, query: str) -> bool:
    haystack = (
        json.dumps(value, ensure_ascii=False, default=str)
        if not isinstance(value, str)
        else value
    ).casefold()
    terms = [x for x in query.casefold().split() if x]
    return all(term in haystack for term in terms)


async def _fetch_new_page(page: int) -> Any:
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "apiKey": _api_key(),
        "User-Agent": "lietuvos-viesieji-pirkimai/4.0",
    }
    body = {"pageSize": VPT_PAGE_SIZE, "pageNum": page}

    last = None
    for attempt in range(3):
        try:
            async with httpx.AsyncClient(timeout=_timeout(), follow_redirects=True) as client:
                r = await client.post(VPT_API_URL, headers=headers, json=body)

            if r.status_code >= 400:
                raise RuntimeError(f"Naujos CVP IS API HTTP {r.status_code}: {r.text[:800]}")
            return r.json()

        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            last = exc
            if attempt < 2:
                await asyncio.sleep(1.5 * (attempt + 1))

    raise RuntimeError(f"Nepavyko pasiekti naujos CVP IS API: {last!r}")


async def _search_new_cvpis(query: str, limit: int) -> dict[str, Any]:
    results = []
    scanned = 0
    pages_scanned = 0
    semaphore = asyncio.Semaphore(SEARCH_CONCURRENCY)

    async def one(page: int):
        async with semaphore:
            try:
                return page, await _fetch_new_page(page)
            except Exception:
                return page, None

    batch_size = 10

    for start in range(1, MAX_NEW_PAGES + 1, batch_size):
        pages = list(range(start, min(start + batch_size, MAX_NEW_PAGES + 1)))
        batch = await asyncio.gather(*(one(p) for p in pages))

        good = 0
        empty_pages = 0

        for page, payload in batch:
            if payload is None:
                continue

            good += 1
            pages_scanned += 1
            records = _json_records(payload)

            if not records:
                empty_pages += 1
                continue

            for record in records:
                scanned += 1
                if _matches_text(record, query):
                    results.append({"source": "Nauja CVP IS API", "url": None, "data": record})
                    if len(results) >= limit:
                        return {"pages_scanned": pages_scanned, "records_scanned": scanned, "items": results}

        if good and empty_pages == good:
            break

    return {"pages_scanned": pages_scanned, "records_scanned": scanned, "items": results}


def _cvpp_search_url(query: str, page: int) -> str:
    params = {
        "Query": query,
        "IncludeExpired": "true",
        "pageNumber": str(page),
        "pageSize": "100",
    }
    return CVPP_BASE_URL + "?" + urlencode(params)


async def _fetch_cvpp_search_page(query: str, page: int) -> str:
    headers = {
        "Accept": "text/html,application/xhtml+xml",
        "User-Agent": "Mozilla/5.0 Lietuvos-viesieji-pirkimai-paieska/4.0",
    }
    url = _cvpp_search_url(query, page)

    async with httpx.AsyncClient(timeout=_timeout(), follow_redirects=True) as client:
        r = await client.get(url, headers=headers)

    if r.status_code >= 400:
        raise RuntimeError(f"CVPP paieška HTTP {r.status_code}")

    return r.text


def _absolute_cvpp_url(href: str) -> str:
    if href.startswith("http://") or href.startswith("https://"):
        return href
    if href.startswith("/"):
        return "https://cvpp.eviesiejipirkimai.lt" + href
    return "https://cvpp.eviesiejipirkimai.lt/" + href.lstrip("/")


def _extract_cvpp_results(page_html: str, query: str) -> list[dict[str, Any]]:
    hits = []
    seen = set()

    patterns = [
        r'(?is)<a[^>]+href=["\']([^"\']*Notice/Details/[^"\']+)["\'][^>]*>(.*?)</a>',
        r'(?is)<a[^>]+href=["\']([^"\']*ReportsOrProtocol/Details/[^"\']+)["\'][^>]*>(.*?)</a>',
        r'(?is)<a[^>]+href=["\']([^"\']*Contract/Details/[^"\']+)["\'][^>]*>(.*?)</a>',
    ]

    for pattern in patterns:
        for href, title_html in re.findall(pattern, page_html):
            url = _absolute_cvpp_url(html.unescape(href))
            if url in seen:
                continue

            title = _strip_tags(title_html)
            if not title:
                continue

            seen.add(url)
            hits.append({"source": "Senas CVPP", "title": title, "url": url})

    plain = _strip_tags(page_html)
    if not hits and _matches_text(plain, query):
        lower = plain.casefold()
        q = query.casefold()
        pos = lower.find(q)
        if pos < 0:
            pos = 0
        snippet = plain[max(0, pos - 350): min(len(plain), pos + 1000)]

        hits.append({
            "source": "Senas CVPP",
            "title": f"CVPP paieškos rezultatai pagal „{query}“",
            "url": _cvpp_search_url(query, 1),
            "snippet": snippet,
        })

    return hits


async def _search_old_cvpp(query: str, limit: int) -> dict[str, Any]:
    results = []
    pages_scanned = 0

    for page in range(1, MAX_CVPP_RESULT_PAGES + 1):
        try:
            page_html = await _fetch_cvpp_search_page(query, page)
        except Exception as exc:
            return {
                "pages_scanned": pages_scanned,
                "items": results,
                "warning": f"{type(exc).__name__}: {exc}",
            }

        pages_scanned += 1
        page_hits = _extract_cvpp_results(page_html, query)

        if not page_hits:
            if page == 1:
                break
            continue

        for item in page_hits:
            results.append(item)
            if len(results) >= limit:
                return {"pages_scanned": pages_scanned, "items": results, "warning": None}

    return {"pages_scanned": pages_scanned, "items": results, "warning": None}


async def _search_all(query: str, limit: int = 25) -> dict[str, Any]:
    if not query.strip():
        raise ValueError("Paieškos frazė negali būti tuščia.")

    limit = min(max(int(limit), 1), 100)

    new_res, old_res = await asyncio.gather(
        _search_new_cvpis(query, limit),
        _search_old_cvpp(query, limit),
    )

    combined = []
    seen = set()

    for group in (new_res, old_res):
        for item in group.get("items", []):
            signature = item.get("url") or json.dumps(item.get("data", item), ensure_ascii=False, default=str)
            if signature in seen:
                continue
            seen.add(signature)
            combined.append(item)
            if len(combined) >= limit:
                break
        if len(combined) >= limit:
            break

    return {
        "query": query,
        "matches": len(combined),
        "items": combined,
        "sources": {
            "new_cvpis": {
                "pages_scanned": new_res.get("pages_scanned", 0),
                "records_scanned": new_res.get("records_scanned", 0),
                "matches": len(new_res.get("items", [])),
            },
            "old_cvpp": {
                "pages_scanned": old_res.get("pages_scanned", 0),
                "matches": len(old_res.get("items", [])),
                "warning": old_res.get("warning"),
            },
        },
    }


@mcp.tool()
async def search_procurements(query: str, limit: int = 25) -> str:
    """Ieškoti naujoje CVP IS ir senajame CVPP."""
    result = await _search_all(query, limit)
    return json.dumps(result, ensure_ascii=False, indent=2, default=str)


@mcp.tool()
async def cvpis_page(page_num: int = 1, page_size: int = 20) -> str:
    data = await _fetch_new_page(page_num)
    return json.dumps(data, ensure_ascii=False, indent=2)


@mcp.tool()
def procurement_sources() -> str:
    return json.dumps(
        {
            "new_cvpis": "https://viesiejipirkimai.lt/",
            "new_cvpis_api": VPT_API_URL,
            "old_cvpp": CVPP_BASE_URL,
            "vpt": "https://vpt.lrv.lt/",
        },
        ensure_ascii=False,
        indent=2,
    )


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
form{{display:grid;grid-template-columns:1fr 150px 110px;gap:10px}}
input,button{{font:inherit;padding:12px;border-radius:9px;border:1px solid #bcc5cf}}
button{{background:#111827;color:#fff;cursor:pointer}}
small{{color:#5b6673;display:block;margin-top:8px}}
pre{{white-space:pre-wrap;word-break:break-word;background:#f8fafc;padding:14px;border-radius:8px;overflow:auto}}
.result{{border-top:1px solid #e5e7eb;padding:16px 0}}
.error{{color:#9b1c1c;background:#fff1f2;padding:14px;border-radius:8px;white-space:pre-wrap}}
.badge{{display:inline-block;padding:5px 9px;background:#eef2ff;border-radius:999px;margin:3px}}
a{{color:#174ea6}}
@media(max-width:700px){{form{{grid-template-columns:1fr}}}}
</style></head><body><main>{body}</main></body></html>"""


@mcp.custom_route("/", methods=["GET"])
async def web_search(request: Request) -> HTMLResponse:
    query = (request.query_params.get("q") or "").strip()

    try:
        limit = min(max(int(request.query_params.get("limit", "25")), 1), 100)
    except ValueError:
        limit = 25

    form = f"""
<div class="card">
<h1>Lietuvos viešųjų pirkimų paieška</h1>
<p>Tikrinama naujoji CVP IS ir tiesioginė senojo CVPP paieška.</p>
<form method="get" action="/">
<input name="q" value="{html.escape(query)}"
       placeholder="Pvz. Rudkasa, Vakarų verslas, Kretingos sniego valymas..." autofocus>
<input type="number" name="limit" min="1" max="100" value="{limit}" title="Maks. rezultatų">
<button type="submit">Ieškoti</button>
</form>
<small>Senasis CVPP ieškomas tiesiogiai pagal paieškos frazę ir įtraukiant pasibaigusius skelbimus.</small>
</div>"""

    if not query:
        return HTMLResponse(_page(form))

    try:
        result = await _search_all(query, limit)
    except Exception as exc:
        detail = f"{type(exc).__name__}: {exc}\n\n{exc!r}"
        return HTMLResponse(
            _page(form + f'<div class="card"><h2>Paieškos klaida</h2><div class="error">{html.escape(detail)}</div></div>'),
            status_code=500,
        )

    s = result["sources"]
    badges = (
        f'<span class="badge">Nauja CVP IS: {s["new_cvpis"]["matches"]}</span>'
        f'<span class="badge">Senas CVPP: {s["old_cvpp"]["matches"]}</span>'
        f'<span class="badge">Iš viso: {result["matches"]}</span>'
    )

    rows = []

    for i, item in enumerate(result["items"], 1):
        source = html.escape(str(item.get("source", "")))
        url = item.get("url")
        title = item.get("title")
        snippet = item.get("snippet")
        data = item.get("data")

        body = f"<p><strong>Šaltinis:</strong> {source}</p>"

        if title:
            body += f"<p><strong>{html.escape(str(title))}</strong></p>"

        if url:
            safe_url = html.escape(str(url), quote=True)
            body += f'<p><a href="{safe_url}" target="_blank" rel="noopener">Atidaryti pirminį šaltinį</a></p>'

        if snippet:
            body += f"<p>{html.escape(str(snippet))}</p>"

        if data is not None:
            pretty = html.escape(json.dumps(data, ensure_ascii=False, indent=2, default=str))
            body += f"<pre>{pretty}</pre>"

        rows.append(f'<div class="result"><h3>Rezultatas {i}</h3>{body}</div>')

    if not rows:
        rows.append(
            "<p>Atitikmenų nerasta. Pabandykite trumpesnį pavadinimo variantą, "
            "pirkimo numerį, organizaciją arba kitą tiekėjo pavadinimo formą.</p>"
        )

    warning = s["old_cvpp"].get("warning")
    warn_html = f'<p class="error">CVPP pastaba: {html.escape(str(warning))}</p>' if warning else ""

    summary = f"""
<div class="card">
<h2>Rezultatai</h2>
<p>Paieška: <strong>{html.escape(query)}</strong></p>
<p>{badges}</p>
{warn_html}
{''.join(rows)}
</div>"""

    return HTMLResponse(_page(form + summary))


@mcp.custom_route("/api/search", methods=["GET"])
async def api_search(request: Request) -> JSONResponse:
    query = (request.query_params.get("q") or "").strip()

    try:
        limit = min(max(int(request.query_params.get("limit", "25")), 1), 100)
    except ValueError:
        limit = 25

    try:
        return JSONResponse(await _search_all(query, limit))
    except Exception as exc:
        return JSONResponse(
            {"error": type(exc).__name__, "message": str(exc), "detail": repr(exc)},
            status_code=500,
        )


@mcp.custom_route("/health", methods=["GET"])
async def health(_: Request) -> HTMLResponse:
    return HTMLResponse("OK")


if __name__ == "__main__":
    mcp.settings.host = "0.0.0.0"
    mcp.settings.port = int(os.environ.get("PORT", "10000"))
    mcp.run(transport="streamable-http")
