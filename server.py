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
DEFAULT_KEYWORDS = [
    "Rudkasa",
    "Vakarų verslas",
    "sniego",
    "sniego išvežimas",
    "žiemos priežiūra",
]

VPT_PAGE_SIZE = 20
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "120"))
MAX_NEW_PAGES = int(os.getenv("MAX_NEW_PAGES", "300"))
MAX_CVPP_RESULT_PAGES = int(os.getenv("MAX_CVPP_RESULT_PAGES", "30"))
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


def _contains_all(value: Any, terms: list[str]) -> bool:
    haystack = _text(value).casefold()
    return all(term.casefold() in haystack for term in terms if term.strip())


def _contains_any(value: Any, terms: list[str]) -> list[str]:
    haystack = _text(value).casefold()
    return [term for term in terms if term.strip() and term.casefold() in haystack]


def _keyword_list(raw: str) -> list[str]:
    values = [x.strip() for x in re.split(r"[,\n;]+", raw or "") if x.strip()]
    return values or DEFAULT_KEYWORDS


# ----------------------------
# Naujoji CVP IS
# ----------------------------

async def _fetch_new_page(page: int) -> Any:
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "apiKey": _api_key(),
        "User-Agent": "lietuvos-viesieji-pirkimai/6.0",
    }
    body = {"pageSize": VPT_PAGE_SIZE, "pageNum": page}

    last = None
    for attempt in range(3):
        try:
            async with httpx.AsyncClient(timeout=_timeout(), follow_redirects=True) as client:
                r = await client.post(VPT_API_URL, headers=headers, json=body)

            if r.status_code >= 400:
                raise RuntimeError(
                    f"Naujos CVP IS API HTTP {r.status_code}: {r.text[:800]}"
                )
            return r.json()

        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            last = exc
            if attempt < 2:
                await asyncio.sleep(1.5 * (attempt + 1))

    raise RuntimeError(f"Nepavyko pasiekti naujos CVP IS API: {last!r}")


async def _search_new_cvpis(
    buyer: str,
    keywords: list[str],
    limit: int,
) -> dict[str, Any]:
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

    for start in range(1, MAX_NEW_PAGES + 1, 10):
        pages = list(range(start, min(start + 10, MAX_NEW_PAGES + 1)))
        batch = await asyncio.gather(*(one(p) for p in pages))

        good = 0
        empty_pages = 0

        for _, payload in batch:
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

                if buyer and not _contains_all(record, [buyer]):
                    continue

                matched = _contains_any(record, keywords)

                results.append(
                    {
                        "source": "Nauja CVP IS API",
                        "buyer": buyer,
                        "matched_keywords": matched,
                        "data": record,
                    }
                )

                if len(results) >= limit:
                    return {
                        "pages_scanned": pages_scanned,
                        "records_scanned": scanned,
                        "items": results,
                    }

        if good and empty_pages == good:
            break

    return {
        "pages_scanned": pages_scanned,
        "records_scanned": scanned,
        "items": results,
    }


# ----------------------------
# Senasis CVPP
# ----------------------------

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
        "User-Agent": "Mozilla/5.0 Lietuvos-viesieji-pirkimai-paieska/6.0",
    }

    async with httpx.AsyncClient(timeout=_timeout(), follow_redirects=True) as client:
        r = await client.get(_cvpp_search_url(query, page), headers=headers)

    if r.status_code >= 400:
        raise RuntimeError(f"CVPP paieška HTTP {r.status_code}")

    return r.text


def _absolute_cvpp_url(href: str) -> str:
    if href.startswith("http://") or href.startswith("https://"):
        return href
    if href.startswith("/"):
        return "https://cvpp.eviesiejipirkimai.lt" + href
    return "https://cvpp.eviesiejipirkimai.lt/" + href.lstrip("/")


def _extract_cvpp_results(page_html: str) -> list[dict[str, Any]]:
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
            hits.append(
                {
                    "source": "Senas CVPP",
                    "title": title,
                    "url": url,
                }
            )

    return hits


async def _search_old_cvpp(
    buyer: str,
    keywords: list[str],
    limit: int,
) -> dict[str, Any]:
    results = []
    pages_scanned = 0

    # Pirmiausia ieškoma pagal pirkėją, po to atskirai pagal raktažodžius.
    queries = [buyer] + [f"{buyer} {kw}" for kw in keywords]

    seen_urls = set()

    for query in queries:
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
            page_hits = _extract_cvpp_results(page_html)

            if not page_hits and page == 1:
                break

            for item in page_hits:
                if item["url"] in seen_urls:
                    continue

                seen_urls.add(item["url"])
                matched = _contains_any(item.get("title", ""), keywords)
                item["buyer"] = buyer
                item["matched_keywords"] = matched
                results.append(item)

                if len(results) >= limit:
                    return {
                        "pages_scanned": pages_scanned,
                        "items": results,
                        "warning": None,
                    }

    return {
        "pages_scanned": pages_scanned,
        "items": results,
        "warning": None,
    }


# ----------------------------
# Mano konkursas
# ----------------------------

def _parse_mano_konkursas_export(text: str, content_type: str) -> list[Any]:
    ct = (content_type or "").casefold()

    if "json" in ct or text.lstrip().startswith(("{", "[")):
        payload = json.loads(text)
        return _json_records(payload)

    return list(csv.DictReader(io.StringIO(text)))


async def _search_mano_konkursas(
    buyer: str,
    keywords: list[str],
    limit: int,
) -> dict[str, Any]:
    if not MANO_KONKURSAS_EXPORT_URL:
        return {
            "items": [],
            "status": "not_connected",
            "message": (
                "„Mano konkursas“ eksportas dar neprijungtas. "
                "Kai bus gauta eksporto arba oficialios integracijos nuoroda, "
                "Render aplinkoje nustatykite MANO_KONKURSAS_EXPORT_URL."
            ),
        }

    headers = {
        "Accept": "application/json,text/csv,text/plain,*/*",
        "User-Agent": "Lietuvos-viesieji-pirkimai-paieska/6.0",
    }

    try:
        async with httpx.AsyncClient(timeout=_timeout(), follow_redirects=True) as client:
            r = await client.get(MANO_KONKURSAS_EXPORT_URL, headers=headers)

        if r.status_code >= 400:
            return {
                "items": [],
                "status": "error",
                "message": f"„Mano konkursas“ eksportas grąžino HTTP {r.status_code}.",
            }

        records = _parse_mano_konkursas_export(
            r.text,
            r.headers.get("content-type", ""),
        )

        hits = []

        for record in records:
            if buyer and not _contains_all(record, [buyer]):
                continue

            matched = _contains_any(record, keywords)

            hits.append(
                {
                    "source": "Mano konkursas",
                    "url": MANO_KONKURSAS_HOME,
                    "buyer": buyer,
                    "matched_keywords": matched,
                    "data": record,
                }
            )

            if len(hits) >= limit:
                break

        return {
            "items": hits,
            "status": "connected",
            "message": f"Patikrinta „Mano konkursas“ eksporto įrašų: {len(records)}.",
        }

    except Exception as exc:
        return {
            "items": [],
            "status": "error",
            "message": f"{type(exc).__name__}: {exc}",
        }


# ----------------------------
# Bendra paieška
# ----------------------------

async def _search_all(
    buyer: str,
    keywords: list[str],
    limit: int = 50,
) -> dict[str, Any]:
    buyer = buyer.strip() or DEFAULT_BUYER
    limit = min(max(int(limit), 1), 200)

    new_res, old_res, mano_res = await asyncio.gather(
        _search_new_cvpis(buyer, keywords, limit),
        _search_old_cvpp(buyer, keywords, limit),
        _search_mano_konkursas(buyer, keywords, limit),
    )

    combined = []
    seen = set()

    for group in (new_res, old_res, mano_res):
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

        if len(combined) >= limit:
            break

    # Rezultatai su sutapusiais raktažodžiais keliami į viršų.
    combined.sort(
        key=lambda x: len(x.get("matched_keywords", [])),
        reverse=True,
    )

    return {
        "buyer": buyer,
        "keywords": keywords,
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
            "mano_konkursas": {
                "matches": len(mano_res.get("items", [])),
                "status": mano_res.get("status"),
                "message": mano_res.get("message"),
            },
        },
    }


@mcp.tool()
async def search_procurements(
    buyer: str = DEFAULT_BUYER,
    keywords: str = "Rudkasa; Vakarų verslas; sniego; sniego išvežimas; žiemos priežiūra",
    limit: int = 50,
) -> str:
    """
    Surinkti konkrečios perkančiosios organizacijos pirkimus ir
    pažymėti įrašus, kuriuose sutampa nurodyti tiekėjai ar raktažodžiai.
    """
    result = await _search_all(buyer, _keyword_list(keywords), limit)
    return json.dumps(result, ensure_ascii=False, indent=2, default=str)


@mcp.tool()
def procurement_sources() -> str:
    return json.dumps(
        {
            "new_cvpis": "https://viesiejipirkimai.lt/",
            "new_cvpis_api": VPT_API_URL,
            "old_cvpp": CVPP_BASE_URL,
            "mano_konkursas": MANO_KONKURSAS_HOME,
            "mano_konkursas_export_connected": bool(MANO_KONKURSAS_EXPORT_URL),
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
form{{display:grid;grid-template-columns:1fr 1fr 120px;gap:10px}}
input,textarea,button{{font:inherit;padding:12px;border-radius:9px;border:1px solid #bcc5cf}}
textarea{{min-height:48px;resize:vertical}}
button{{background:#111827;color:#fff;cursor:pointer}}
small{{color:#5b6673;display:block;margin-top:8px}}
pre{{white-space:pre-wrap;word-break:break-word;background:#f8fafc;padding:14px;border-radius:8px;overflow:auto}}
.result{{border-top:1px solid #e5e7eb;padding:16px 0}}
.note{{background:#fff8e1;padding:14px;border-radius:8px}}
.error{{color:#9b1c1c;background:#fff1f2;padding:14px;border-radius:8px;white-space:pre-wrap}}
.badge{{display:inline-block;padding:5px 9px;background:#eef2ff;border-radius:999px;margin:3px}}
.hit{{display:inline-block;padding:4px 8px;background:#dcfce7;border-radius:999px;margin:3px}}
a{{color:#174ea6}}
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
        limit = min(max(int(request.query_params.get("limit", "50")), 1), 200)
    except ValueError:
        limit = 50

    form = f"""
<div class="card">
<h1>Lietuvos viešųjų pirkimų paieška</h1>
<p>Pirmiausia surenkami pasirinktos perkančiosios organizacijos pirkimai, tada jų viduje ieškoma tiekėjų ir raktažodžių.</p>
<form method="get" action="/">
<input name="buyer" value="{html.escape(buyer)}" placeholder="Perkančioji organizacija">
<textarea name="keywords" placeholder="Tiekėjai ir raktažodžiai">{html.escape('; '.join(keywords))}</textarea>
<input type="number" name="limit" min="1" max="200" value="{limit}" title="Maks. rezultatų">
<button type="submit">Ieškoti</button>
</form>
<small>Numatytoji organizacija: Kretingos rajono savivaldybės administracija. Raktažodžiai gali būti atskirti kabliataškiais arba kableliais.</small>
</div>"""

    try:
        result = await _search_all(buyer, keywords, limit)
    except Exception as exc:
        detail = f"{type(exc).__name__}: {exc}\n\n{exc!r}"
        return HTMLResponse(
            _page(
                form
                + f'<div class="card"><h2>Paieškos klaida</h2>'
                + f'<div class="error">{html.escape(detail)}</div></div>'
            ),
            status_code=500,
        )

    s = result["sources"]
    badges = (
        f'<span class="badge">Nauja CVP IS: {s["new_cvpis"]["matches"]}</span>'
        f'<span class="badge">Senas CVPP: {s["old_cvpp"]["matches"]}</span>'
        f'<span class="badge">Mano konkursas: {s["mano_konkursas"]["matches"]}</span>'
        f'<span class="badge">Iš viso: {result["matches"]}</span>'
    )

    rows = []

    for i, item in enumerate(result["items"], 1):
        source = html.escape(str(item.get("source", "")))
        url = item.get("url")
        title = item.get("title")
        data = item.get("data")
        matched = item.get("matched_keywords", [])

        body = f"<p><strong>Šaltinis:</strong> {source}</p>"

        if matched:
            body += "<p><strong>Sutapo:</strong> " + "".join(
                f'<span class="hit">{html.escape(str(x))}</span>'
                for x in matched
            ) + "</p>"

        if title:
            body += f"<p><strong>{html.escape(str(title))}</strong></p>"

        if url:
            safe_url = html.escape(str(url), quote=True)
            body += (
                f'<p><a href="{safe_url}" target="_blank" rel="noopener">'
                f'Atidaryti pirminį šaltinį</a></p>'
            )

        if data is not None:
            body += "<pre>" + html.escape(
                json.dumps(data, ensure_ascii=False, indent=2, default=str)
            ) + "</pre>"

        rows.append(
            f'<div class="result"><h3>Rezultatas {i}</h3>{body}</div>'
        )

    if not rows:
        rows.append(
            "<p>Pasirinktos perkančiosios organizacijos pirkimų prijungtuose šaltiniuose nerasta.</p>"
        )

    mano_note = (
        f'<div class="note"><strong>„Mano konkursas“:</strong> '
        f'{html.escape(str(s["mano_konkursas"]["message"]))}</div>'
    )

    cvpp_warning = s["old_cvpp"].get("warning")
    cvpp_note = (
        f'<div class="error">CVPP pastaba: {html.escape(str(cvpp_warning))}</div>'
        if cvpp_warning
        else ""
    )

    summary = f"""
<div class="card">
<h2>Rezultatai</h2>
<p><strong>Perkančioji organizacija:</strong> {html.escape(result["buyer"])}</p>
<p><strong>Stebimi tiekėjai / raktažodžiai:</strong> {html.escape("; ".join(result["keywords"]))}</p>
<p>{badges}</p>
{mano_note}
{cvpp_note}
{''.join(rows)}
</div>"""

    return HTMLResponse(_page(form + summary))


@mcp.custom_route("/api/search", methods=["GET"])
async def api_search(request: Request) -> JSONResponse:
    buyer = (request.query_params.get("buyer") or DEFAULT_BUYER).strip()
    raw_keywords = request.query_params.get("keywords") or ""
    keywords = _keyword_list(raw_keywords)

    try:
        limit = min(max(int(request.query_params.get("limit", "50")), 1), 200)
    except ValueError:
        limit = 50

    try:
        return JSONResponse(await _search_all(buyer, keywords, limit))
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
