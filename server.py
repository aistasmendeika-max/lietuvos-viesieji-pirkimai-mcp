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
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "120"))
MAX_SCAN_PAGES = int(os.getenv("MAX_SCAN_PAGES", "200"))
SEARCH_BATCH_SIZE = int(os.getenv("SEARCH_BATCH_SIZE", "8"))
SEARCH_CONCURRENCY = int(os.getenv("SEARCH_CONCURRENCY", "4"))


def _api_key() -> str:
    key = os.getenv("VPT_API_KEY", "").strip()
    if not key:
        raise RuntimeError(
            "Nenustatytas VPT_API_KEY. Įrašykite jį Render → Environment."
        )
    return key


def _client_timeout() -> httpx.Timeout:
    return httpx.Timeout(
        connect=20.0,
        read=HTTP_TIMEOUT,
        write=30.0,
        pool=20.0,
    )


async def _fetch_page(page_num: int, page_size: int = VPT_PAGE_SIZE) -> Any:
    if page_num < 1:
        raise ValueError("page_num turi būti >= 1")

    page_size = min(max(int(page_size), 1), VPT_PAGE_SIZE)

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "apiKey": _api_key(),
        "User-Agent": "lietuvos-viesieji-pirkimai-mcp/2.0",
    }
    payload = {"pageSize": page_size, "pageNum": page_num}

    last_error: Exception | None = None

    for attempt in range(3):
        try:
            async with httpx.AsyncClient(
                timeout=_client_timeout(),
                follow_redirects=True,
                http2=False,
            ) as client:
                response = await client.post(
                    VPT_API_URL,
                    headers=headers,
                    json=payload,
                )

            if response.status_code >= 400:
                body = response.text[:1500]
                raise RuntimeError(
                    f"CVP IS API grąžino HTTP {response.status_code}. "
                    f"Atsakymas: {body or '(tuščias)'}"
                )

            try:
                return response.json()
            except Exception as exc:
                body = response.text[:1500]
                raise RuntimeError(
                    "CVP IS API grąžino ne JSON atsakymą. "
                    f"Content-Type={response.headers.get('content-type')!r}; "
                    f"atsakymas={body!r}"
                ) from exc

        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            last_error = exc
            if attempt < 2:
                await asyncio.sleep(1.5 * (attempt + 1))
                continue
            raise RuntimeError(
                "Nepavyko prisijungti prie CVP IS API po 3 bandymų. "
                f"Klaida: {type(exc).__name__}: {exc!r}"
            ) from exc

    raise RuntimeError(f"CVP IS API klaida: {last_error!r}")


def _records(payload: Any) -> list[Any]:
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, dict):
        return [payload]

    for key in (
        "content", "items", "results", "data", "records",
        "procurements", "cfts", "result",
    ):
        value = payload.get(key)
        if isinstance(value, list):
            return value
        if isinstance(value, dict):
            nested = _records(value)
            if nested:
                return nested

    return [payload]


def _possible_total_pages(payload: Any) -> int | None:
    if not isinstance(payload, dict):
        return None

    candidates = (
        "totalPages", "total_pages", "pageCount", "pages",
        "totalPageCount", "numberOfPages",
    )
    for key in candidates:
        value = payload.get(key)
        if isinstance(value, int) and value > 0:
            return value

    for key in ("page", "pagination", "meta"):
        nested = payload.get(key)
        if isinstance(nested, dict):
            found = _possible_total_pages(nested)
            if found:
                return found

    return None


def _match_record(record: Any, query: str) -> bool:
    haystack = json.dumps(record, ensure_ascii=False, default=str).casefold()
    terms = [t for t in query.casefold().split() if t]
    return all(term in haystack for term in terms)


async def _search_records(
    query: str,
    limit: int = 25,
    max_pages: int | None = None,
) -> dict[str, Any]:
    query = query.strip()
    if not query:
        raise ValueError("Paieškos frazė negali būti tuščia.")
    if not 1 <= limit <= 100:
        raise ValueError("Rezultatų limitas turi būti nuo 1 iki 100.")

    max_pages = max_pages or MAX_SCAN_PAGES
    max_pages = min(max(int(max_pages), 1), 500)

    matches: list[Any] = []
    scanned = 0
    pages_scanned = 0
    known_total_pages: int | None = None
    semaphore = asyncio.Semaphore(max(1, SEARCH_CONCURRENCY))

    async def fetch_one(page: int) -> tuple[int, Any]:
        async with semaphore:
            return page, await _fetch_page(page, VPT_PAGE_SIZE)

    current = 1
    while current <= max_pages and len(matches) < limit:
        end = min(current + SEARCH_BATCH_SIZE - 1, max_pages)
        if known_total_pages:
            end = min(end, known_total_pages)

        page_numbers = list(range(current, end + 1))
        if not page_numbers:
            break

        batch = await asyncio.gather(
            *(fetch_one(p) for p in page_numbers),
            return_exceptions=True,
        )

        successful_pages = 0
        for item in batch:
            if isinstance(item, Exception):
                # Vieno puslapio sutrikimas nestabdo visos paieškos.
                continue

            page_num, payload = item
            successful_pages += 1
            pages_scanned += 1

            total_pages = _possible_total_pages(payload)
            if total_pages:
                known_total_pages = total_pages

            records = _records(payload)
            if not records:
                continue

            for record in records:
                scanned += 1
                if _match_record(record, query):
                    matches.append(record)
                    if len(matches) >= limit:
                        break

            if len(matches) >= limit:
                break

        if successful_pages == 0:
            raise RuntimeError(
                "Nepavyko gauti nė vieno CVP IS API puslapio šiame paieškos etape."
            )

        if known_total_pages and end >= known_total_pages:
            break

        current = end + 1

    return {
        "query": query,
        "pages_scanned": pages_scanned,
        "records_scanned": scanned,
        "matches": len(matches),
        "limit": limit,
        "max_pages": max_pages,
        "known_total_pages": known_total_pages,
        "items": matches,
    }


@mcp.tool()
async def cvpis_page(page_num: int = 1, page_size: int = 20) -> str:
    """Gauti vieną naujosios CVP IS viešo API pirkimų rezultatų puslapį."""
    data = await _fetch_page(page_num, page_size)
    return json.dumps(data, ensure_ascii=False, indent=2)


@mcp.tool()
async def search_cvpis(
    query: str,
    limit: int = 25,
    max_pages: int = 200,
) -> str:
    """
    Automatiškai ieškoti frazės CVP IS API puslapiuose.
    Paieška eina per puslapius paketais iki kol randa limitą arba pasiekia ribą.
    """
    result = await _search_records(
        query=query,
        limit=limit,
        max_pages=max_pages,
    )
    return json.dumps(result, ensure_ascii=False, indent=2, default=str)


@mcp.tool()
def procurement_sources() -> str:
    """Pagrindinės oficialios Lietuvos viešųjų pirkimų duomenų nuorodos."""
    return json.dumps(
        {
            "new_cvpis": "https://viesiejipirkimai.lt/",
            "new_cvpis_api": VPT_API_URL,
            "old_cvpp": "https://cvpp.eviesiejipirkimai.lt/",
            "open_data": "https://data.gov.lt/",
            "vpt": "https://vpt.lrv.lt/",
        },
        ensure_ascii=False,
        indent=2,
    )


def _page(body: str) -> str:
    return f"""<!doctype html>
<html lang="lt">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Lietuvos viešųjų pirkimų paieška</title>
<style>
body {{ font-family: Arial, sans-serif; margin: 0; background: #f4f6f8; color: #17212b; }}
main {{ max-width: 1180px; margin: 40px auto; padding: 0 20px; }}
.card {{ background: white; border-radius: 14px; padding: 24px; box-shadow: 0 3px 16px rgba(0,0,0,.08); margin-bottom: 20px; }}
h1 {{ margin-top: 0; }}
form {{ display: grid; grid-template-columns: 1fr 160px 110px; gap: 10px; }}
input, button {{ font: inherit; padding: 12px; border-radius: 9px; border: 1px solid #bcc5cf; }}
button {{ background: #111827; color: white; cursor: pointer; }}
small {{ color: #5b6673; display: block; margin-top: 8px; }}
pre {{ white-space: pre-wrap; word-break: break-word; background: #f8fafc; padding: 14px; border-radius: 8px; overflow: auto; }}
.result {{ border-top: 1px solid #e5e7eb; padding: 16px 0; }}
.error {{ color: #9b1c1c; background: #fff1f2; padding: 14px; border-radius: 8px; white-space: pre-wrap; }}
.badge {{ display:inline-block; padding:4px 8px; background:#eef2ff; border-radius:999px; margin-right:8px; }}
@media (max-width: 700px) {{ form {{ grid-template-columns: 1fr; }} }}
</style>
</head>
<body><main>{body}</main></body>
</html>"""


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
<p>Automatinė paieška naujosios CVP IS viešo API duomenyse.</p>
<form method="get" action="/">
<input name="q" value="{html.escape(query)}"
       placeholder="Pvz. Rudkasa, Kretingos rajono savivaldybė, sniego išvežimas..." autofocus>
<input type="number" name="limit" min="1" max="100" value="{limit}"
       title="Maksimalus rezultatų skaičius">
<button type="submit">Ieškoti</button>
</form>
<small>
Sistema pati tikrina CVP IS puslapius paketais po {SEARCH_BATCH_SIZE},
iki {MAX_SCAN_PAGES} puslapių (iki {MAX_SCAN_PAGES * VPT_PAGE_SIZE} įrašų)
arba kol surenka nustatytą rezultatų skaičių.
</small>
</div>
"""

    if not query:
        return HTMLResponse(_page(form))

    try:
        result = await _search_records(
            query=query,
            limit=limit,
            max_pages=MAX_SCAN_PAGES,
        )
    except Exception as exc:
        detail = (
            f"{type(exc).__name__}: {exc}\n\n"
            f"Techninė informacija: {exc!r}"
        )
        return HTMLResponse(
            _page(
                form
                + '<div class="card"><h2>Paieškos klaida</h2>'
                + f'<div class="error">{html.escape(detail)}</div></div>'
            ),
            status_code=500,
        )

    rows = []
    for idx, item in enumerate(result["items"], start=1):
        pretty = html.escape(
            json.dumps(item, ensure_ascii=False, indent=2, default=str)
        )
        rows.append(
            f'<div class="result"><strong>Rezultatas {idx}</strong><pre>{pretty}</pre></div>'
        )

    if not rows:
        rows.append(
            '<p>Atitikmenų nerasta patikrintoje CVP IS duomenų dalyje. '
            'Galima bandyti trumpesnę frazę arba kitą pavadinimo variantą.</p>'
        )

    total_text = (
        f'<span class="badge">Patikrinta puslapių: {result["pages_scanned"]}</span>'
        f'<span class="badge">Patikrinta įrašų: {result["records_scanned"]}</span>'
        f'<span class="badge">Rasta: {result["matches"]}</span>'
    )

    summary = f"""
<div class="card">
<h2>Rezultatai</h2>
<p>Paieška: <strong>{html.escape(query)}</strong></p>
<p>{total_text}</p>
{''.join(rows)}
</div>
"""
    return HTMLResponse(_page(form + summary))


@mcp.custom_route("/api/search", methods=["GET"])
async def api_search(request: Request) -> JSONResponse:
    query = (request.query_params.get("q") or "").strip()
    try:
        limit = min(max(int(request.query_params.get("limit", "25")), 1), 100)
    except ValueError:
        limit = 25

    try:
        result = await _search_records(query=query, limit=limit)
        return JSONResponse(result)
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
