import json
import os
from typing import Any

import httpx
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP

load_dotenv()

mcp = FastMCP("Lietuvos viešieji pirkimai")

VPT_API_URL = os.getenv(
    "VPT_API_URL",
    "https://viesiejipirkimai.lt/epps-integration/api/cft-details-export",
)
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "45"))


def _api_key() -> str:
    key = os.getenv("VPT_API_KEY", "").strip()
    if not key:
        raise RuntimeError(
            "Nenustatytas VPT_API_KEY. Nukopijuokite .env.example į .env "
            "ir įrašykite VPT CVP IS API raktą."
        )
    return key


async def _fetch_page(page_num: int, page_size: int) -> Any:
    if page_num < 1:
        raise ValueError("page_num turi būti >= 1")
    if not 1 <= page_size <= 100:
        raise ValueError("page_size turi būti nuo 1 iki 100")

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "apiKey": _api_key(),
        "User-Agent": "lietuvos-viesieji-pirkimai-mcp/1.0",
    }
    payload = {"pageSize": page_size, "pageNum": page_num}

    async with httpx.AsyncClient(
        timeout=HTTP_TIMEOUT,
        follow_redirects=True,
    ) as client:
        response = await client.post(VPT_API_URL, headers=headers, json=payload)
        response.raise_for_status()
        return response.json()


def _records(payload: Any) -> list[Any]:
    """Tolerantiškai ištraukia įrašų masyvą iš galimų API atsakymo formų."""
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
            nested = _records(value)
            if nested:
                return nested
    return [payload]


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
    page_size: int = 50,
    limit: int = 50,
) -> str:
    """
    Ieškoti frazės naujosios CVP IS API įrašuose.

    Paieška atliekama keliuose API puslapiuose ir tikrina visą kiekvieno
    įrašo JSON tekstą, todėl tinka organizacijos, tiekėjo, pirkimo numerio,
    BVPŽ kodo ar raktažodžio paieškai.
    """
    query = query.strip()
    if not query:
        raise ValueError("query negali būti tuščias")
    if not 1 <= pages <= 50:
        raise ValueError("pages turi būti nuo 1 iki 50")
    if not 1 <= limit <= 500:
        raise ValueError("limit turi būti nuo 1 iki 500")

    needle = query.casefold()
    matches: list[Any] = []
    scanned = 0

    for page in range(start_page, start_page + pages):
        payload = await _fetch_page(page, page_size)
        records = _records(payload)
        if not records:
            break

        for record in records:
            scanned += 1
            haystack = json.dumps(record, ensure_ascii=False, default=str).casefold()
            if needle in haystack:
                matches.append(record)
                if len(matches) >= limit:
                    break

        if len(matches) >= limit:
            break

    result = {
        "query": query,
        "start_page": start_page,
        "pages_requested": pages,
        "records_scanned": scanned,
        "matches": len(matches),
        "items": matches,
    }
    return json.dumps(result, ensure_ascii=False, indent=2, default=str)


@mcp.tool()
def procurement_sources() -> str:
    """Pagrindinės oficialios Lietuvos viešųjų pirkimų duomenų nuorodos."""
    return json.dumps(
        {
            "new_cvpis": "https://viesiejipirkimai.lt/",
            "new_cvpis_api": VPT_API_URL,
            "old_cvpp": "https://cvpp.eviesiejipirkimai.lt/",
            "open_data": "https://data.gov.lt/datasets/2867/",
            "vpt": "https://vpt.lrv.lt/",
        },
        ensure_ascii=False,
        indent=2,
    )


if __name__ == "__main__":
    mcp.run()
