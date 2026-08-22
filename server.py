import asyncio
import csv
import html
import io
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
MANO_KONKURSAS_HOME = "https://www.manokonkursas.lt/"
MANO_KONKURSAS_EXPORT_URL = os.getenv("MANO_KONKURSAS_EXPORT_URL", "").strip()

DEFAULT_BUYER = "Kretingos rajono savivaldybės administracija"

VPT_PAGE_SIZE = 20
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "90"))
BATCH_PAGES = int(os.getenv("BATCH_PAGES", "25"))
SEARCH_CONCURRENCY = int(os.getenv("SEARCH_CONCURRENCY", "5"))
MAX_CVPP_PAGES_PER_SEARCH = int(os.getenv("MAX_CVPP_PAGES_PER_SEARCH", "10"))


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


def _normalize(value: str) -> str:
    return re.sub(r"\s+", " ", value.casefold()).strip()


def _buyer_match(value: Any, buyer: str) -> bool:
    haystack = _normalize(_text(value))
    buyer_norm = _normalize(buyer)

    if buyer_norm in haystack:
        return True

    core_terms = [
        term for term in re.split(r"\s+", buyer_norm)
        if len(term) >= 5 and term not in {"rajono", "savivaldybės", "administracija"}
    ]
    if not core_terms:
        core_terms = [term for term in re.split(r"\s+", buyer_norm) if len(term) >= 5]

    return all(term in haystack for term in core_terms)


def _keyword_match(value: Any, keyword: str) -> bool:
    if not keyword.strip():
        return True
    return _normalize(keyword) in _normalize(_text(value))


async def _fetch_new_page(page: int) -> Any:
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "apiKey": _api_key(),
        "User-Agent": "lietuvos-viesieji-pirkimai/8.0",
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


async def _scan_cvpis_batch(
    buyer: str,
    keyword: str,
    start_page: int,
    pages: int,
    limit: int,
) -> dict[str, Any]:
    semaphore = asyncio.Semaphore(max(1, SEARCH_CONCURRENCY))

    async def one(page: int):
        async with semaphore:
            try:
                return page, await _fetch_new_page(page), None
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

            if not _buyer_match(record, buyer):
                continue

            if not _keyword_match(record, keyword):
                continue

            results.append(
                {
                    "source": "Nauja CVP IS",
                    "page": page_num,
                    "data": record,
                }
            )

            if len(results) >= limit:
                break

        if len(results) >= limit:
            break

    return {
        "source": "Nauja CVP IS",
        "buyer": buyer,
        "keyword": keyword,
        "start_page": start_page,
        "pages_requested": pages,
        "pages_ok": pages_ok,
        "records_scanned": scanned,
        "matches": len(results),
        "items": results,
        "next_page": start_page + pages,
        "errors": errors[:10],
    }


def _cvpp_search_url(query: str, page: int) -> str:
    params = {
        "Query": query,
        "IncludeExpired": "true",
        "pageNumber": str(page),
        "pageSize": "100",
    }
    return CVPP_BASE_URL + "?" + urlencode(params)


async def _fetch_cvpp_page(query: str, page: int) -> str:
    headers = {
        "Accept": "text/html,application/xhtml+xml",
        "User-Agent": "Mozilla/5.0 Lietuvos-viesieji-pirkimai-paieska/8.0",
    }

    async with httpx.AsyncClient(timeout=_timeout(), follow_redirects=True) as client:
        r = await client.get(_cvpp_search_url(query, page), headers=headers)

    if r.status_code >= 400:
        raise RuntimeError(f"CVPP paieška HTTP {r.status_code}")

    return r.text


def _strip_tags(text: str) -> str:
    text = re.sub(r"(?is)<script.*?>.*?</script>", " ", text)
    text = re.sub(r"(?is)<style.*?>.*?</style>", " ", text)
    text = re.sub(r"(?s)<[^>]+>", " ", text)
    text = html.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def _absolute_cvpp_url(href: str) -> str:
    if href.startswith("http://") or href.startswith("https://"):
        return href
    if href.startswith("/"):
        return "https://cvpp.eviesiejipirkimai.lt" + href
    return "https://cvpp.eviesiejipirkimai.lt/" + href.lstrip("/")


def _extract_cvpp_results(page_html: str, keyword: str) -> list[dict[str, Any]]:
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

            if keyword.strip() and _normalize(keyword) not in _normalize(title):
                continue

            seen.add(url)
            hits.append(
                {
                    "source": "Senas CVPP",
                    "title": title,
                    "url": url,
                }
            )

    return hits


async def _search_cvpp(
    buyer: str,
    keyword: str,
    limit: int,
) -> dict[str, Any]:
    results = []
    pages_scanned = 0
    warning = None

    queries = [buyer]
    if keyword.strip():
        queries.append(f"{buyer} {keyword}")

    seen = set()

    for query in queries:
        for page in range(1, MAX_CVPP_PAGES_PER_SEARCH + 1):
            try:
                page_html = await _fetch_cvpp_page(query, page)
            except Exception as exc:
                warning = f"{type(exc).__name__}: {exc}"
                break

            pages_scanned += 1
            hits = _extract_cvpp_results(page_html, keyword)

            if not hits and page == 1:
                break

            for item in hits:
                if item["url"] in seen:
                    continue
                seen.add(item["url"])
                results.append(item)

                if len(results) >= limit:
                    return {
                        "source": "Senas CVPP",
                        "pages_scanned": pages_scanned,
                        "matches": len(results),
                        "items": results,
                        "warning": warning,
                    }

    return {
        "source": "Senas CVPP",
        "pages_scanned": pages_scanned,
        "matches": len(results),
        "items": results,
        "warning": warning,
    }


def _parse_mano_export(text: str, content_type: str) -> list[Any]:
    ct = (content_type or "").casefold()

    if "json" in ct or text.lstrip().startswith(("{", "[")):
        payload = json.loads(text)
        return _json_records(payload)

    return list(csv.DictReader(io.StringIO(text)))


async def _search_mano_konkursas(
    buyer: str,
    keyword: str,
    limit: int,
) -> dict[str, Any]:
    if not MANO_KONKURSAS_EXPORT_URL:
        return {
            "source": "Mano konkursas",
            "status": "not_connected",
            "matches": 0,
            "items": [],
            "message": (
                "„Mano konkursas“ eksportas dar neprijungtas. "
                "Kai bus turima oficiali eksporto ar integracijos nuoroda, "
                "ją galima įrašyti Render aplinkoje kaip MANO_KONKURSAS_EXPORT_URL."
            ),
        }

    headers = {
        "Accept": "application/json,text/csv,text/plain,*/*",
        "User-Agent": "Lietuvos-viesieji-pirkimai-paieska/8.0",
    }

    try:
        async with httpx.AsyncClient(timeout=_timeout(), follow_redirects=True) as client:
            r = await client.get(MANO_KONKURSAS_EXPORT_URL, headers=headers)

        if r.status_code >= 400:
            return {
                "source": "Mano konkursas",
                "status": "error",
                "matches": 0,
                "items": [],
                "message": f"„Mano konkursas“ eksportas grąžino HTTP {r.status_code}.",
            }

        records = _parse_mano_export(r.text, r.headers.get("content-type", ""))
        hits = []

        for record in records:
            if not _buyer_match(record, buyer):
                continue

            if not _keyword_match(record, keyword):
                continue

            hits.append(
                {
                    "source": "Mano konkursas",
                    "url": MANO_KONKURSAS_HOME,
                    "data": record,
                }
            )

            if len(hits) >= limit:
                break

        return {
            "source": "Mano konkursas",
            "status": "connected",
            "matches": len(hits),
            "items": hits,
            "message": f"Patikrinta eksporto įrašų: {len(records)}.",
        }

    except Exception as exc:
        return {
            "source": "Mano konkursas",
            "status": "error",
            "matches": 0,
            "items": [],
            "message": f"{type(exc).__name__}: {exc}",
        }


async def _search_all(
    buyer: str,
    keyword: str,
    start_page: int,
    limit: int,
) -> dict[str, Any]:
    cvpis, cvpp, mano = await asyncio.gather(
        _scan_cvpis_batch(
            buyer=buyer,
            keyword=keyword,
            start_page=start_page,
            pages=BATCH_PAGES,
            limit=limit,
        ),
        _search_cvpp(
            buyer=buyer,
            keyword=keyword,
            limit=limit,
        ),
        _search_mano_konkursas(
            buyer=buyer,
            keyword=keyword,
            limit=limit,
        ),
    )

    combined = []
    seen = set()

    for group in (cvpis, cvpp, mano):
        for item in group.get("items", []):
            signature = item.get("url") or json.dumps(
                item.get("data", item),
                ensure_ascii=False,
                default=str,
            )
            if signature in seen:
                continue
            seen.add(signature)
            combined.append(item)
            if len(combined) >= limit:
                break

    return {
        "buyer": buyer,
        "keyword": keyword,
        "matches": len(combined),
        "items": combined,
        "next_page": cvpis["next_page"],
        "sources": {
            "cvpis": cvpis,
            "cvpp": cvpp,
            "mano_konkursas": mano,
        },
    }


@mcp.tool()
async def search_administration_documents(
    buyer: str = DEFAULT_BUYER,
    keyword: str = "",
    start_page: int = 1,
    limit: int = 50,
) -> str:
    """
    Ieško pasirinktos administracijos pirkimų ir dokumentų.
    Papildomas raktažodis neprivalomas.
    """
    result = await _search_all(
        buyer=buyer.strip() or DEFAULT_BUYER,
        keyword=keyword.strip(),
        start_page=max(1, int(start_page)),
        limit=max(1, min(int(limit), 200)),
    )
    return json.dumps(result, ensure_ascii=False, indent=2, default=str)


def _page(body: str) -> str:
    return f"""<!doctype html>
<html lang="lt"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Kretingos administracijos pirkimų paieška</title>
<style>
body{{font-family:Arial,sans-serif;margin:0;background:#f4f6f8;color:#17212b}}
main{{max-width:1180px;margin:36px auto;padding:0 20px}}
.card{{background:#fff;border-radius:14px;padding:24px;box-shadow:0 3px 16px rgba(0,0,0,.08);margin-bottom:20px}}
h1{{margin-top:0}}
form{{display:grid;grid-template-columns:1fr 1fr 120px;gap:10px}}
input,button{{font:inherit;padding:12px;border-radius:9px;border:1px solid #bcc5cf}}
button{{background:#111827;color:#fff;cursor:pointer}}
small{{color:#5b6673;display:block;margin-top:8px}}
pre{{white-space:pre-wrap;word-break:break-word;background:#f8fafc;padding:14px;border-radius:8px;overflow:auto}}
.result{{border-top:1px solid #e5e7eb;padding:16px 0}}
.badge{{display:inline-block;padding:5px 9px;background:#eef2ff;border-radius:999px;margin:3px}}
.note{{background:#fff8e1;padding:14px;border-radius:8px}}
.error{{color:#9b1c1c;background:#fff1f2;padding:14px;border-radius:8px;white-space:pre-wrap}}
.actions{{display:flex;gap:10px;flex-wrap:wrap;margin-top:16px}}
a.button{{display:inline-block;text-decoration:none;background:#334155;color:white;padding:11px 14px;border-radius:9px}}
@media(max-width:800px){{form{{grid-template-columns:1fr}}}}
</style></head><body><main>{body}</main></body></html>"""


@mcp.custom_route("/", methods=["GET"])
async def web_search(request: Request) -> HTMLResponse:
    buyer = (request.query_params.get("buyer") or DEFAULT_BUYER).strip()
    keyword = (request.query_params.get("keyword") or "").strip()

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
<h1>Kretingos rajono savivaldybės administracijos pirkimų paieška</h1>
<p>Ieškoma pagal perkančiąją organizaciją. Papildomas raktažodis neprivalomas.</p>
<form method="get" action="/">
<input name="buyer" value="{html.escape(buyer)}" placeholder="Perkančioji organizacija">
<input name="keyword" value="{html.escape(keyword)}" placeholder="Papildomas raktažodis, jei reikia">
<input type="number" name="limit" min="1" max="200" value="{limit}" title="Maks. rezultatų">
<input type="hidden" name="start_page" value="1">
<input type="hidden" name="run" value="1">
<button type="submit">Ieškoti</button>
</form>
<small>Jei papildomo raktažodžio neįrašysite, bus ieškoma visų rastų Administracijos pirkimų ir dokumentų.</small>
</div>"""

    if not run_search:
        return HTMLResponse(_page(form))

    try:
        result = await _search_all(
            buyer=buyer,
            keyword=keyword,
            start_page=start_page,
            limit=limit,
        )
    except Exception as exc:
        return HTMLResponse(
            _page(
                form
                + f'<div class="card"><h2>Paieškos klaida</h2>'
                + f'<div class="error">{html.escape(type(exc).__name__ + ": " + str(exc))}</div></div>'
            ),
            status_code=500,
        )

    s = result["sources"]
    badges = (
        f'<span class="badge">Nauja CVP IS: {s["cvpis"]["matches"]}</span>'
        f'<span class="badge">Senas CVPP: {s["cvpp"]["matches"]}</span>'
        f'<span class="badge">Mano konkursas: {s["mano_konkursas"]["matches"]}</span>'
        f'<span class="badge">Iš viso: {result["matches"]}</span>'
    )

    rows = []
    for i, item in enumerate(result["items"], 1):
        source = html.escape(str(item.get("source", "")))
        body = f"<p><strong>Šaltinis:</strong> {source}</p>"

        if item.get("title"):
            body += f"<p><strong>{html.escape(str(item['title']))}</strong></p>"

        if item.get("url"):
            safe_url = html.escape(str(item["url"]), quote=True)
            body += (
                f'<p><a href="{safe_url}" target="_blank" rel="noopener">'
                f'Atidaryti pirminį šaltinį</a></p>'
            )

        if item.get("data") is not None:
            body += "<pre>" + html.escape(
                json.dumps(item["data"], ensure_ascii=False, indent=2, default=str)
            ) + "</pre>"

        rows.append(f'<div class="result"><h3>Rezultatas {i}</h3>{body}</div>')

    if not rows:
        rows.append("<p>Šiame etape atitikmenų nerasta.</p>")

    notes = []

    if s["mano_konkursas"]["status"] != "connected":
        notes.append(
            f'<div class="note"><strong>„Mano konkursas“:</strong> '
            f'{html.escape(str(s["mano_konkursas"]["message"]))}</div>'
        )

    if s["cvpp"].get("warning"):
        notes.append(
            f'<div class="error"><strong>CVPP:</strong> '
            f'{html.escape(str(s["cvpp"]["warning"]))}</div>'
        )

    if s["cvpis"]["errors"]:
        notes.append(
            '<div class="note"><strong>CVP IS:</strong> '
            + "<br>".join(html.escape(x) for x in s["cvpis"]["errors"])
            + "</div>"
        )

    next_url = (
        "/?"
        + urlencode(
            {
                "buyer": buyer,
                "keyword": keyword,
                "limit": str(limit),
                "start_page": str(result["next_page"]),
                "run": "1",
            }
        )
    )

    summary = f"""
<div class="card">
<h2>Rezultatai</h2>
<p><strong>Perkančioji organizacija:</strong> {html.escape(buyer)}</p>
<p><strong>Papildomas raktažodis:</strong> {html.escape(keyword) if keyword else "nenaudojamas"}</p>
<p>{badges}</p>
{''.join(notes)}
{''.join(rows)}
<div class="actions">
<a class="button" href="{html.escape(next_url, quote=True)}">Ieškoti toliau nuo {result["next_page"]} psl.</a>
<a class="button" href="/">Nauja paieška</a>
</div>
</div>"""

    return HTMLResponse(_page(form + summary))


@mcp.custom_route("/api/search", methods=["GET"])
async def api_search(request: Request) -> JSONResponse:
    buyer = (request.query_params.get("buyer") or DEFAULT_BUYER).strip()
    keyword = (request.query_params.get("keyword") or "").strip()

    try:
        start_page = max(1, int(request.query_params.get("start_page", "1")))
    except ValueError:
        start_page = 1

    try:
        limit = max(1, min(int(request.query_params.get("limit", "50")), 200))
    except ValueError:
        limit = 50

    try:
        return JSONResponse(
            await _search_all(
                buyer=buyer,
                keyword=keyword,
                start_page=start_page,
                limit=limit,
            )
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
