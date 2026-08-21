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
from starlette.responses import HTMLResponse

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


def _api_key() -> str:
    key = os.getenv("VPT_API_KEY", "").strip()
    if not key:
        raise RuntimeError(
            "Nenustatytas VPT_API_KEY. Įrašykite jį Render → Environment."
        )
    return key


async def _fetch_page(page_num: int, page_size: int = VPT_PAGE_SIZE) -> Any:
    if page_num < 1:
        raise ValueError("page_num turi būti >= 1")

    page_size = min(max(int(page_size), 1), VPT_PAGE_SIZE)

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "apiKey": _api_key(),
        "User-Agent": "lietuvos-viesieji-pirkimai-mcp/1.2",
    }
    payload = {"pageSize": page_size, "pageNum": page_num}

    timeout = httpx.Timeout(
        connect=20.0,
        read=HTTP_TIMEOUT,
        write=30.0,
        pool=20.0,
    )

    last_error: Exception | None = None

    for attempt in range(3):
        try:
            async with httpx.AsyncClient(
                timeout=timeout,
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
                await asyncio.sleep(2 * (attempt + 1))
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
        "content",
        "items",
        "results",
        "data",
        "records",
        "procurements",
        "cfts",
        "result",
    ):
        value = payload.get(key)
        if isinstance(value, list):
            return value
        if isinstance(value, dict):
            nested = _records(value)
            if nested:
                return nested

    return [payload]


async def _search_records(
    query: str,
    start_page: int = 1,
    pages: int = 5,
    page_size: int = VPT_PAGE_SIZE,
    limit: int = 50,
) -> dict[str, Any]:
    query = query.strip()
    if not query:
        raise ValueError("Paieškos frazė negali būti tuščia.")
    if not 1 <= pages <= 50:
        raise ValueError("Puslapių skaičius turi būti nuo 1 iki 50.")
    if not 1 <= limit <= 500:
        raise ValueError("Rezultatų limitas turi būti nuo 1 iki 500.")

    needle = query.casefold()
    matches: list[Any] = []
    scanned = 0
    pages_scanned = 0

    for page in range(start_page, start_page + pages):
        payload = await _fetch_page(page, page_size)
        records = _records(payload)
        pages_scanned += 1

        if not records:
            break

        for record in records:
            scanned += 1
            haystack = json.dumps(
                record,
                ensure_ascii=False,
                default=str,
            ).casefold()

            if needle in haystack:
                matches.append(record)
                if len(matches) >= limit:
                    break

        if len(matches) >= limit:
            break

    return {
        "query": query,
        "pages_scanned": pages_scanned,
        "records_scanned": scanned,
        "matches": len(matches),
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
    start_page: int = 1,
    pages: int = 5,
    page_size: int = 20,
    limit: int = 50,
) -> str:
    """Ieškoti frazės naujosios CVP IS API įrašuose."""
    result = await _search_records(
        query=query,
        start_page=start_page,
        pages=pages,
        page_size=page_size,
        limit=limit,
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
main {{ max-width: 1100px; margin: 40px auto; padding: 0 20px; }}
.card {{ background: white; border-radius: 14px; padding: 24px; box-shadow: 0 3px 16px rgba(0,0,0,.08); margin-bottom: 20px; }}
h1 {{ margin-top: 0; }}
form {{ display: grid; grid-template-columns: 1fr 120px 120px 110px; gap: 10px; }}
input, button {{ font: inherit; padding: 12px; border-radius: 9px; border: 1px solid #bcc5cf; }}
button {{ background: #111827; color: white; cursor: pointer; }}
small {{ color: #5b6673; }}
pre {{ white-space: pre-wrap; word-break: break-word; background: #f8fafc; padding: 14px; border-radius: 8px; overflow: auto; }}
.result {{ border-top: 1px solid #e5e7eb; padding: 16px 0; }}
.error {{ color: #9b1c1c; background: #fff1f2; padding: 14px; border-radius: 8px; white-space: pre-wrap; }}
@media (max-width: 700px) {{ form {{ grid-template-columns: 1fr; }} }}
</style>
</head>
<body><main>{body}</main></body>
</html>"""


@mcp.custom_route("/", methods=["GET"])
async def web_search(request: Request) -> HTMLResponse:
    query = (request.query_params.get("q") or "").strip()

    try:
        pages = min(max(int(request.query_params.get("pages", "5")), 1), 50)
    except ValueError:
        pages = 5

    try:
        limit = min(max(int(request.query_params.get("limit", "25")), 1), 100)
    except ValueError:
        limit = 25

    form = f"""
<div class="card">
<h1>Lietuvos viešųjų pirkimų paieška</h1>
<p>Paieška naujosios CVP IS viešo API duomenyse.</p>
<form method="get" action="/">
<input name="q" value="{html.escape(query)}" placeholder="Pvz. Kretingos rajono savivaldybė, Rudkasa, sniego..." autofocus>
<input type="number" name="pages" min="1" max="50" value="{pages}" title="API puslapių skaičius">
<input type="number" name="limit" min="1" max="100" value="{limit}" title="Maks. rezultatų">
<button type="submit">Ieškoti</button>
</form>
<small>Pirmas skaičius – kiek CVP IS API puslapių po 20 įrašų tikrinti; antras – maksimalus rezultatų skaičius.</small>
</div>
"""

    if not query:
        return HTMLResponse(_page(form))

    try:
        result = await _search_records(
            query=query,
            pages=pages,
            page_size=VPT_PAGE_SIZE,
            limit=limit,
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
            '<p>Nurodytuose API puslapiuose atitikmenų nerasta. '
            'Pabandykite didinti tikrinamų puslapių skaičių arba keisti paieškos frazę.</p>'
        )

    summary = f"""
<div class="card">
<h2>Rezultatai</h2>
<p>Paieška: <strong>{html.escape(query)}</strong></p>
<p>Patikrinta API puslapių: {result["pages_scanned"]}; įrašų: {result["records_scanned"]}; rasta: {result["matches"]}.</p>
{''.join(rows)}
</div>
"""
    return HTMLResponse(_page(form + summary))


@mcp.custom_route("/health", methods=["GET"])
async def health(_: Request) -> HTMLResponse:
    return HTMLResponse("OK")


if __name__ == "__main__":
    mcp.settings.host = "0.0.0.0"
    mcp.settings.port = int(os.environ.get("PORT", "10000"))
    mcp.run(transport="streamable-http")
