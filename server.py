import asyncio
import csv
import html
import io
import json
import os
import re
from dataclasses import dataclass, asdict
from typing import Any
from urllib.parse import urlencode, urljoin, urlparse

import httpx
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse

load_dotenv()

APP_NAME = "Lietuvos viešieji pirkimai"
DEFAULT_BUYER = "Kretingos rajono savivaldybės administracija"

CVPIS_BASE = "https://viesiejipirkimai.lt/"
CVPP_BASE = "https://cvpp.eviesiejipirkimai.lt/"
VPT_API_URL = os.getenv(
    "VPT_API_URL",
    "https://viesiejipirkimai.lt/epps-integration/api/cft-details-export",
)
MANO_KONKURSAS_EXPORT_URL = os.getenv("MANO_KONKURSAS_EXPORT_URL", "").strip()

HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "75"))
VPT_PAGE_SIZE = 20
SEARCH_BATCH_PAGES = int(os.getenv("SEARCH_BATCH_PAGES", "20"))
MAX_RESULT_ITEMS = int(os.getenv("MAX_RESULT_ITEMS", "100"))
MAX_DOC_PAGES = int(os.getenv("MAX_DOC_PAGES", "12"))
MAX_DOC_LINKS = int(os.getenv("MAX_DOC_LINKS", "40"))

mcp = FastMCP(
    APP_NAME,
    transport_security=TransportSecuritySettings(
        allowed_hosts=[
            "lietuvos-viesieji-pirkimai-mcp.onrender.com",
            "lietuvos-viesieji-pirkimai-mcp.onrender.com:*",
        ],
        allowed_origins=["https://lietuvos-viesieji-pirkimai-mcp.onrender.com"],
    ),
)


def _timeout() -> httpx.Timeout:
    return httpx.Timeout(connect=20.0, read=HTTP_TIMEOUT, write=25.0, pool=20.0)


def _api_key() -> str:
    key = os.getenv("VPT_API_KEY", "").strip()
    if not key:
        raise RuntimeError("Nenustatytas VPT_API_KEY Render → Environment.")
    return key


def _norm(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").casefold()).strip()


def _text(value: Any) -> str:
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, default=str)


def _strip_tags(value: str) -> str:
    value = re.sub(r"(?is)<script.*?>.*?</script>", " ", value)
    value = re.sub(r"(?is)<style.*?>.*?</style>", " ", value)
    value = re.sub(r"(?s)<[^>]+>", " ", value)
    value = html.unescape(value)
    return re.sub(r"\s+", " ", value).strip()


def _buyer_match(value: Any, buyer: str) -> bool:
    haystack = _norm(_text(value))
    target = _norm(buyer)
    if target in haystack:
        return True
    terms = [
        t for t in re.split(r"\s+", target)
        if len(t) >= 5 and t not in {"rajono", "savivaldybės", "administracija"}
    ]
    return bool(terms) and all(t in haystack for t in terms)


def _keyword_match(value: Any, keyword: str) -> bool:
    return not keyword.strip() or _norm(keyword) in _norm(_text(value))


def _json_records(payload: Any) -> list[Any]:
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, dict):
        return [payload]
    for key in ("content", "items", "results", "data", "records", "procurements", "cfts", "result"):
        val = payload.get(key)
        if isinstance(val, list):
            return val
        if isinstance(val, dict):
            nested = _json_records(val)
            if nested:
                return nested
    return [payload]


def _filename_from_disposition(disposition: str) -> str:
    if not disposition:
        return ""
    m = re.search(r"filename\*\s*=\s*UTF-8''([^;]+)", disposition, re.I)
    if m:
        from urllib.parse import unquote
        return unquote(m.group(1).strip().strip('"'))
    m = re.search(r'filename\s*=\s*"?([^";]+)"?', disposition, re.I)
    return m.group(1).strip() if m else ""


def _is_file_like(url: str, label: str = "") -> bool:
    blob = f"{url} {label}".casefold()
    return any(x in blob for x in (
        ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".zip", ".rar", ".7z",
        "downloadnotice", "downloadfile", "attachment", "downloadattachment"
    ))

def _classify(label: str, url: str, filename: str = "", content_type: str = "") -> str:
    blob = f"{label} {url} {filename}".casefold()
    ctype = (content_type or "").casefold()
    if "text/html" in ctype:
        return "HTML puslapis"
    if any(x in blob for x in (
        "contract award notice", "award notice", "skelbimas apie sutarties skyrimą",
        "laimėtoj", "winner", "award", "rezultat"
    )):
        return "rezultatai / laimėtojas"
    if any(x in blob for x in ("voluntary ex-ante", "prior information notice", "notice", "skelbim")):
        return "skelbimas"
    if any(x in blob for x in ("atsakym", "paaiškin", "clarification")):
        return "paaiškinimas / atsakymas"
    if any(x in blob for x in ("tdp", "projekt", "technin")):
        return "techninis dokumentas"
    if any(x in blob for x in ("sutart", "rangos sutart", "pirkimo sutart")):
        return "sutartis"
    if "contract" in blob and "notice" not in blob and "award" not in blob:
        return "sutartis"
    return "kitas dokumentas"

def _rank_document(item: dict[str, Any]) -> int:
    blob = " ".join(str(item.get(k, "")) for k in (
        "label", "url", "final_url", "filename", "content_type", "category"
    )).casefold()
    score = 0
    category = item.get("category")
    if category == "sutartis":
        score += 500
    elif category == "rezultatai / laimėtojas":
        score += 250
    elif category == "skelbimas":
        score += 80
    elif category == "paaiškinimas / atsakymas":
        score += 20
    elif category == "techninis dokumentas":
        score += 10
    if "simra" in blob:
        score += 120
    if ".pdf" in blob or "application/pdf" in blob:
        score += 30
    if item.get("http_status") == 200:
        score += 3
    return score


@dataclass
class ProcResult:
    source: str
    title: str = ""
    url: str = ""
    data: Any = None
    resource_id: str = ""


async def _fetch_new_api_page(client: httpx.AsyncClient, page: int) -> Any:
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "apiKey": _api_key(),
        "User-Agent": "lietuvos-viesieji-pirkimai/15.0",
    }
    body = {"pageSize": VPT_PAGE_SIZE, "pageNum": page}
    r = await client.post(VPT_API_URL, headers=headers, json=body)
    if r.status_code >= 400:
        raise RuntimeError(f"CVP IS API HTTP {r.status_code}: {r.text[:500]}")
    return r.json()


def _extract_resource_id(value: Any) -> str:
    text = _text(value)
    for pattern in (
        r"resourceId[=:186\"'\s]+(\d+)",
        r'"resourceId"\s*:\s*"?(\d+)"?',
        r"resourceId=(\d+)",
    ):
        m = re.search(pattern, text, re.I)
        if m:
            return m.group(1)
    return ""


async def search_new_cvpis(buyer: str, keyword: str, start_page: int, limit: int) -> dict[str, Any]:
    sem = asyncio.Semaphore(4)
    async with httpx.AsyncClient(
        timeout=_timeout(), follow_redirects=True, http2=False,
        limits=httpx.Limits(max_keepalive_connections=3, max_connections=5),
    ) as client:
        async def fetch(page: int):
            async with sem:
                try:
                    return page, await _fetch_new_api_page(client, page), None
                except Exception as exc:
                    return page, None, f"{type(exc).__name__}: {exc}"
        pages = list(range(start_page, start_page + SEARCH_BATCH_PAGES))
        responses = await asyncio.gather(*(fetch(p) for p in pages))

    items, errors = [], []
    scanned = 0
    for page, payload, error in responses:
        if error:
            errors.append(f"p. {page}: {error}")
            continue
        for record in _json_records(payload):
            scanned += 1
            if not _buyer_match(record, buyer) or not _keyword_match(record, keyword):
                continue
            items.append(asdict(ProcResult(
                source="Nauja CVP IS", data=record, resource_id=_extract_resource_id(record)
            )))
            if len(items) >= limit:
                break
        if len(items) >= limit:
            break

    return {
        "source": "Nauja CVP IS", "items": items, "matches": len(items),
        "records_scanned": scanned, "next_page": start_page + SEARCH_BATCH_PAGES,
        "errors": errors[:10],
    }


async def search_old_cvpp(buyer: str, keyword: str, limit: int) -> dict[str, Any]:
    query = buyer if not keyword.strip() else f"{buyer} {keyword}"
    url = CVPP_BASE + "?" + urlencode({
        "Query": query, "IncludeExpired": "true", "pageNumber": "1", "pageSize": "100"
    })
    headers = {"User-Agent": "Mozilla/5.0 Lietuvos-viesieji-pirkimai-paieska/15.0"}
    try:
        async with httpx.AsyncClient(timeout=_timeout(), follow_redirects=True, headers=headers) as client:
            r = await client.get(url)
        if r.status_code >= 400:
            raise RuntimeError(f"CVPP HTTP {r.status_code}")
    except Exception as exc:
        return {"source": "Senas CVPP", "items": [], "matches": 0, "warning": f"{type(exc).__name__}: {exc}"}

    hits, seen = [], set()
    page = r.text[:3_000_000]
    patterns = [
        r'(?is)<a[^>]+href=["\']([^"\']*Notice/Details/[^"\']+)["\'][^>]*>(.*?)</a>',
        r'(?is)<a[^>]+href=["\']([^"\']*ReportsOrProtocol/Details/[^"\']+)["\'][^>]*>(.*?)</a>',
        r'(?is)<a[^>]+href=["\']([^"\']*Contract/Details/[^"\']+)["\'][^>]*>(.*?)</a>',
    ]
    for pattern in patterns:
        for href, label_html in re.findall(pattern, page):
            title = _strip_tags(label_html)
            absolute = urljoin(CVPP_BASE, html.unescape(href))
            if absolute in seen or (keyword.strip() and _norm(keyword) not in _norm(title)):
                continue
            seen.add(absolute)
            hits.append(asdict(ProcResult(source="Senas CVPP", title=title, url=absolute)))
            if len(hits) >= limit:
                break
    return {"source": "Senas CVPP", "items": hits, "matches": len(hits), "warning": None}


def _parse_export(text: str, content_type: str) -> list[Any]:
    if "json" in (content_type or "").casefold() or text.lstrip().startswith(("{", "[")):
        return _json_records(json.loads(text))
    return list(csv.DictReader(io.StringIO(text)))


async def search_mano_konkursas(buyer: str, keyword: str, limit: int) -> dict[str, Any]:
    if not MANO_KONKURSAS_EXPORT_URL:
        return {"source": "Mano konkursas", "status": "not_connected", "items": [], "matches": 0, "message": "Šaltinis neprijungtas."}
    try:
        async with httpx.AsyncClient(timeout=_timeout(), follow_redirects=True) as client:
            r = await client.get(MANO_KONKURSAS_EXPORT_URL)
        if r.status_code >= 400:
            raise RuntimeError(f"HTTP {r.status_code}")
        records = _parse_export(r.text, r.headers.get("content-type", ""))
        hits = []
        for rec in records:
            if _buyer_match(rec, buyer) and _keyword_match(rec, keyword):
                hits.append(asdict(ProcResult(source="Mano konkursas", data=rec)))
            if len(hits) >= limit:
                break
        return {"source": "Mano konkursas", "status": "connected", "items": hits, "matches": len(hits), "message": f"Patikrinta įrašų: {len(records)}."}
    except Exception as exc:
        return {"source": "Mano konkursas", "status": "error", "items": [], "matches": 0, "message": f"{type(exc).__name__}: {exc}"}


async def _session_client() -> httpx.AsyncClient:
    client = httpx.AsyncClient(
        timeout=_timeout(), follow_redirects=True, http2=False,
        headers={"User-Agent": "Mozilla/5.0 Lietuvos-viesieji-pirkimai-paieska/15.0", "Accept": "text/html,application/xhtml+xml,application/pdf,*/*"},
        limits=httpx.Limits(max_keepalive_connections=2, max_connections=3),
    )
    try:
        await client.get(urljoin(CVPIS_BASE, "epps/home.do"))
    except Exception:
        pass
    return client


def _extract_internal_links(page_html: str, base_url: str, resource_id: str) -> list[dict[str, str]]:
    links, seen = [], set()
    pattern = r"""(?is)<a[^>]+href=["']([^"']+)["'][^>]*>(.*?)</a>"""
    for href, label_html in re.findall(pattern, page_html):
        label = _strip_tags(label_html)[:300]
        absolute = urljoin(base_url, html.unescape(href.strip()))
        if absolute in seen:
            continue
        seen.add(absolute)
        if "viesiejipirkimai.lt" not in urlparse(absolute).netloc.casefold():
            continue
        low = absolute.casefold()
        if resource_id not in absolute and "/epps/notices/downloadnoticefores.do?" not in low:
            continue
        links.append({"url": absolute, "label": label})
    return links

def _page_role(url: str, label: str = "") -> str:
    blob = f"{url} {label}".casefold()
    if "contract award notice" in blob or any(x in blob for x in ("award", "winner", "result")):
        return "rezultatai / laimėtojas"
    if "listcontractdocuments.do" in blob or "document" in blob:
        return "pirkimo dokumentai"
    if "notice" in blob or "skelbim" in blob:
        return "skelbimas"
    if "contract" in blob and "award" not in blob and "notice" not in blob:
        return "sutartis / sutarties duomenys"
    return "kitas puslapis"

async def inspect_procurement(resource_id: str) -> dict[str, Any]:
    resource_id = str(resource_id).strip()
    if not resource_id.isdigit():
        raise ValueError("resourceId turi būti skaičius.")

    seeds = [
        f"{CVPIS_BASE}epps/cft/prepareViewCfTWS.do?resourceId={resource_id}",
        f"{CVPIS_BASE}epps/cft/listContractDocuments.do?resourceId={resource_id}",
        f"{CVPIS_BASE}epps/cft/downloadNoticeForAdvSearch.do?resourceId={resource_id}",
    ]

    pages, documents = [], []
    visited, seen_doc_keys = set(), set()
    queued = [(u, 0, "pradinis") for u in seeds]

    client = await _session_client()
    try:
        while queued and len(visited) < MAX_DOC_PAGES:
            url, depth, source_label = queued.pop(0)
            if url in visited:
                continue
            visited.add(url)
            try:
                r = await client.get(url)
                final_url = str(r.url)
                ctype = r.headers.get("content-type", "")
                disposition = r.headers.get("content-disposition", "")
                filename = _filename_from_disposition(disposition)
                pages.append({
                    "url": url, "final_url": final_url, "status": r.status_code,
                    "content_type": ctype, "filename": filename,
                    "role": _page_role(final_url, source_label),
                })
                if r.status_code >= 400:
                    continue

                if "text/html" not in ctype.casefold():
                    key = filename or final_url
                    if key not in seen_doc_keys:
                        seen_doc_keys.add(key)
                        documents.append({
                            "label": source_label or filename or "Dokumentas",
                            "url": final_url, "final_url": final_url, "filename": filename,
                            "content_type": ctype, "http_status": r.status_code,
                            "category": _classify(source_label, final_url, filename, ctype),
                        })
                    continue

                body = r.text[:2_500_000]
                pattern = r'(?is)<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>'
                for href, label_html in re.findall(pattern, body):
                    label = _strip_tags(label_html)[:300]
                    absolute = urljoin(final_url, html.unescape(href.strip()))
                    if _is_file_like(absolute, label):
                        key = f"{absolute}|{label}"
                        if key not in seen_doc_keys:
                            seen_doc_keys.add(key)
                            documents.append({"label": label or "Dokumentas", "url": absolute, "category": _classify(label, absolute, "", "")})

                if depth < 2:
                    for link in _extract_internal_links(body, final_url, resource_id):
                        role = _page_role(link["url"], link["label"])
                        if role in ("rezultatai / laimėtojas", "sutartis / sutarties duomenys", "pirkimo dokumentai", "skelbimas"):
                            queued.append((link["url"], depth + 1, link["label"] or role))
            except Exception as exc:
                pages.append({"url": url, "status": "error", "error": f"{type(exc).__name__}: {exc}"})
    finally:
        await client.aclose()

    sem = asyncio.Semaphore(3)
    async def probe(item: dict[str, Any]) -> dict[str, Any]:
        result = dict(item)
        async with sem:
            try:
                async with httpx.AsyncClient(
                    timeout=_timeout(), follow_redirects=True, http2=False,
                    limits=httpx.Limits(max_keepalive_connections=2, max_connections=3),
                    headers={"User-Agent": "Mozilla/5.0 Lietuvos-viesieji-pirkimai-paieska/15.0"},
                ) as c:
                    r = await c.head(item["url"])
                    if r.status_code in (403, 405):
                        async with c.stream("GET", item["url"]) as sr:
                            result["http_status"] = sr.status_code
                            result["final_url"] = str(sr.url)
                            result["content_type"] = sr.headers.get("content-type", "")
                            result["filename"] = _filename_from_disposition(sr.headers.get("content-disposition", ""))
                    else:
                        result["http_status"] = r.status_code
                        result["final_url"] = str(r.url)
                        result["content_type"] = r.headers.get("content-type", "")
                        result["filename"] = _filename_from_disposition(r.headers.get("content-disposition", ""))
                result["category"] = _classify(result.get("label", ""), result.get("final_url") or result.get("url", ""), result.get("filename", ""), result.get("content_type", ""))
            except Exception as exc:
                result["probe_error"] = f"{type(exc).__name__}: {exc}"
            result["score"] = _rank_document(result)
            return result

    probed = await asyncio.gather(*(probe(x) for x in documents[:MAX_DOC_LINKS]))
    probed = [x for x in probed if "text/html" not in str(x.get("content_type") or "").casefold()]
    deduped, seen = [], set()
    for item in sorted(probed, key=lambda x: x.get("score", 0), reverse=True):
        key = _norm(item.get("filename")) or _norm(item.get("final_url")) or _norm(item.get("url"))
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(item)

    contracts = [x for x in deduped if x.get("category") == "sutartis"][:10]
    awards = [x for x in deduped if x.get("category") == "rezultatai / laimėtojas"][:10]
    return {
        "resource_id": resource_id,
        "pages_checked": pages,
        "documents": deduped,
        "contracts": contracts,
        "award_documents": awards,
        "summary": {
            "pages_checked": len(pages),
            "unique_documents": len(deduped),
            "contracts_found": len(contracts),
            "award_documents_found": len(awards),
            "contract_status": (
                "Rasta bent viena tikėtina pasirašyta sutartis."
                if contracts else
                "Pasirašytos sutarties failas šiame viešame CVP IS kelyje nerastas."
            ),
        },
    }


async def search_all(buyer: str, keyword: str, start_page: int, limit: int) -> dict[str, Any]:
    new, old, mano = await asyncio.gather(
        search_new_cvpis(buyer, keyword, start_page, limit),
        search_old_cvpp(buyer, keyword, limit),
        search_mano_konkursas(buyer, keyword, limit),
    )
    combined, seen = [], set()
    for group in (new, old, mano):
        for item in group.get("items", []):
            key = item.get("url") or item.get("resource_id") or _text(item.get("data"))
            key = _norm(key)
            if not key or key in seen:
                continue
            seen.add(key)
            combined.append(item)
            if len(combined) >= limit:
                break
    return {
        "buyer": buyer, "keyword": keyword, "items": combined, "matches": len(combined),
        "next_page": new.get("next_page", start_page + SEARCH_BATCH_PAGES),
        "sources": {"cvpis": new, "cvpp": old, "mano_konkursas": mano},
    }


@mcp.tool()
async def search_administration_documents(buyer: str = DEFAULT_BUYER, keyword: str = "", start_page: int = 1, limit: int = 50) -> str:
    result = await search_all(buyer.strip() or DEFAULT_BUYER, keyword.strip(), max(1, int(start_page)), max(1, min(int(limit), MAX_RESULT_ITEMS)))
    return json.dumps(result, ensure_ascii=False, indent=2, default=str)


@mcp.tool()
async def inspect_procurement_documents(resource_id: str) -> str:
    return json.dumps(await inspect_procurement(resource_id), ensure_ascii=False, indent=2, default=str)


def _page(body: str) -> str:
    return f'''<!doctype html><html lang="lt"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>Lietuvos viešųjų pirkimų paieška</title><style>
body{{font-family:Arial,sans-serif;margin:0;background:#f4f6f8;color:#17212b}}main{{max-width:1180px;margin:34px auto;padding:0 20px}}.card{{background:#fff;border-radius:14px;padding:24px;box-shadow:0 3px 16px rgba(0,0,0,.08);margin-bottom:20px}}h1,h2{{margin-top:0}}form{{display:grid;grid-template-columns:1fr 1fr 120px;gap:10px}}input,button{{font:inherit;padding:12px;border-radius:9px;border:1px solid #bcc5cf}}button,.button{{background:#111827;color:#fff;cursor:pointer;text-decoration:none;display:inline-block;padding:11px 14px;border-radius:9px}}small{{color:#5b6673;display:block;margin-top:8px}}pre{{white-space:pre-wrap;word-break:break-word;background:#f8fafc;padding:14px;border-radius:8px;overflow:auto}}.result{{border-top:1px solid #e5e7eb;padding:16px 0}}.badge{{display:inline-block;padding:5px 9px;background:#eef2ff;border-radius:999px;margin:3px}}.good{{background:#ecfdf5}}.note{{background:#fff8e1}}.error{{background:#fff1f2;color:#9b1c1c}}.doc{{padding:12px 0;border-bottom:1px solid #e5e7eb}}@media(max-width:800px){{form{{grid-template-columns:1fr}}}}</style></head><body><main>{body}</main></body></html>'''


@mcp.custom_route("/", methods=["GET"])
async def home(request: Request) -> HTMLResponse:
    buyer = (request.query_params.get("buyer") or DEFAULT_BUYER).strip()
    keyword = (request.query_params.get("keyword") or "").strip()
    run = request.query_params.get("run") == "1"
    try:
        start_page = max(1, int(request.query_params.get("start_page", "1")))
    except ValueError:
        start_page = 1

    form = f'''<div class="card"><h1>Lietuvos viešųjų pirkimų paieška</h1><p>Pirmiausia ieškoma pagal perkančiąją organizaciją. Raktažodis neprivalomas.</p><form method="get"><input name="buyer" value="{html.escape(buyer)}"><input name="keyword" value="{html.escape(keyword)}" placeholder="Papildomas raktažodis"><input type="number" name="limit" value="50" min="1" max="{MAX_RESULT_ITEMS}"><input type="hidden" name="start_page" value="{start_page}"><input type="hidden" name="run" value="1"><button>Ieškoti</button></form></div>
<div class="card"><h2>Išanalizuoti konkretų CVP IS pirkimą</h2><form method="get" action="/procurement"><input name="resource_id" placeholder="resourceId, pvz. 2722748"><div></div><div></div><button>Analizuoti visą pirkimą</button></form><small>Analizuojami skelbimai, pirkimo dokumentai, rezultatų / laimėtojo ir sutarties puslapiai. Nuorodos deduplikuojamos.</small></div>'''
    if not run:
        return HTMLResponse(_page(form))
    try:
        limit = max(1, min(int(request.query_params.get("limit", "50")), MAX_RESULT_ITEMS))
        result = await search_all(buyer, keyword, start_page, limit)
    except Exception as exc:
        return HTMLResponse(_page(form + f'<div class="card error">{html.escape(str(exc))}</div>'), status_code=500)

    rows = []
    for i, item in enumerate(result["items"], 1):
        title = html.escape(str(item.get("title") or "Pirkimas"))
        source = html.escape(str(item.get("source") or ""))
        rid = str(item.get("resource_id") or "")
        body = f"<p><strong>Šaltinis:</strong> {source}</p>"
        if item.get("url"):
            body += f'<p><a href="{html.escape(item["url"], quote=True)}" target="_blank">Atidaryti šaltinį</a></p>'
        if rid:
            body += f'<p><a class="button" href="/procurement?resource_id={html.escape(rid)}">Analizuoti dokumentus</a></p>'
        if item.get("data") is not None:
            body += "<pre>" + html.escape(json.dumps(item["data"], ensure_ascii=False, indent=2, default=str)) + "</pre>"
        rows.append(f'<div class="result"><h3>{i}. {title}</h3>{body}</div>')
    summary = f'''<div class="card"><h2>Rezultatai</h2><p><span class="badge">Nauja CVP IS: {result["sources"]["cvpis"]["matches"]}</span><span class="badge">Senas CVPP: {result["sources"]["cvpp"]["matches"]}</span><span class="badge">Mano konkursas: {result["sources"]["mano_konkursas"]["matches"]}</span><span class="badge">Iš viso: {result["matches"]}</span></p>{''.join(rows) if rows else '<p>Atitikmenų nerasta.</p>'}</div>'''
    return HTMLResponse(_page(form + summary))


@mcp.custom_route("/procurement", methods=["GET"])
async def procurement_page(request: Request) -> HTMLResponse:
    resource_id = (request.query_params.get("resource_id") or "").strip()
    if not resource_id:
        return HTMLResponse(_page('<div class="card error">Nenurodytas resourceId.</div>'), status_code=400)
    try:
        result = await inspect_procurement(resource_id)
    except Exception as exc:
        return HTMLResponse(_page(f'<div class="card error">{html.escape(str(exc))}</div>'), status_code=500)

    s = result["summary"]
    top = f'''<div class="card"><h1>CVP IS pirkimo analizė: {html.escape(resource_id)}</h1><p><span class="badge">Patikrinta puslapių: {s["pages_checked"]}</span><span class="badge">Unikalių dokumentų: {s["unique_documents"]}</span><span class="badge">Sutarčių: {s["contracts_found"]}</span><span class="badge">Rezultatų / laimėtojo dokumentų: {s["award_documents_found"]}</span></p></div>'''

    def render_docs(title: str, docs: list[dict[str, Any]], css: str = "") -> str:
        body = []
        for i, d in enumerate(docs, 1):
            filename = html.escape(str(d.get("filename") or ""))
            label = html.escape(str(d.get("label") or "Dokumentas"))
            category = html.escape(str(d.get("category") or ""))
            url = html.escape(str(d.get("final_url") or d.get("url") or ""), quote=True)
            meta = []
            if filename:
                meta.append(f"<strong>Failas:</strong> {filename}")
            meta.append(f"<strong>Kategorija:</strong> {category}")
            if d.get("content_type"):
                meta.append(f"<strong>Tipas:</strong> {html.escape(str(d['content_type']))}")
            body.append(f'<div class="doc"><strong>{i}. {label}</strong><br>' + "<br>".join(meta) + (f'<br><a href="{url}" target="_blank">Atidaryti dokumentą</a>' if url else "") + "</div>")
        return f'<div class="card {css}"><h2>{title}</h2>{"".join(body) if body else "<p>Nerasta.</p>"}</div>'

    sections = [
        render_docs("Sutartys", result["contracts"], "good"),
        render_docs("Rezultatai / laimėtojas", result["award_documents"], "note"),
        render_docs("Visi unikalūs dokumentai", result["documents"]),
        f'<div class="card"><h2>Techninė eiga</h2><pre>{html.escape(json.dumps(result["pages_checked"], ensure_ascii=False, indent=2))}</pre></div>',
    ]
    return HTMLResponse(_page(top + "".join(sections)))


@mcp.custom_route("/api/procurement", methods=["GET"])
async def procurement_api(request: Request) -> JSONResponse:
    resource_id = (request.query_params.get("resource_id") or "").strip()
    try:
        return JSONResponse(await inspect_procurement(resource_id))
    except Exception as exc:
        return JSONResponse({"error": type(exc).__name__, "message": str(exc)}, status_code=500)


@mcp.custom_route("/health", methods=["GET"])
async def health(_: Request) -> HTMLResponse:
    return HTMLResponse("OK")


if __name__ == "__main__":
    mcp.settings.host = "0.0.0.0"
    mcp.settings.port = int(os.environ.get("PORT", "10000"))
    mcp.run(transport="streamable-http")
