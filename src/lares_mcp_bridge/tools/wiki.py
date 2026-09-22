"""Wiki tools — search, browse and read the house wiki (Wiki.js), read-only.

The wiki holds the durable, human-written knowledge about the house: device
and vendor references, how-it-works notes, digests of external docs. Wiki.js
renders client-side, so without this an agent fetching a page sees a title
and nothing else.

Three reads, one key, no writes. ``search_wiki`` and ``list_wiki_pages`` use
the GraphQL API (``pages.search`` / ``pages.list``), which filter by
``read:pages``. Page *content* takes a different route: Wiki.js 2.x guards the
GraphQL ``pages.single`` / ``pages.singleByPath`` queries with ``manage:pages``
— an edit permission — so a read-only key never gets content that way. The
one read-only route to a page's source is the source view, ``GET
/s/<locale>/<path>`` (``read:source``), which embeds the raw source escaped in
a ``<code v-pre>`` element; ``get_wiki_page`` reads it from there. The key's
group therefore needs exactly ``read:pages`` and ``read:source``.

One shared httpx client, opened in the app lifespan like the DB pool and the
NATS connection; the Bearer token arrives as a mounted Secret file.
"""

from __future__ import annotations

import html
import re
from pathlib import Path
from typing import Any

import httpx

from ..config import Settings
from ..logging_setup import get_logger

log = get_logger(__name__)

_TIMEOUT_SECONDS = 15.0

# source.pug: ``code(v-pre)= page.content`` — escaped, so the body never holds </code>.
_SOURCE_RE = re.compile(r"<code v-pre>(.*?)</code>", re.DOTALL)

_SEARCH_QUERY = """\
query ($query: String!) {
  pages {
    search(query: $query) {
      totalHits
      results { id path locale title description }
    }
  }
}"""

_LIST_QUERY = """\
query {
  pages {
    list(orderBy: PATH) {
      id path locale title description contentType tags createdAt updatedAt
    }
  }
}"""

_client: httpx.AsyncClient | None = None


class WikiError(Exception):
    """The wiki answered, but not with what was asked for."""


async def init(settings: Settings) -> None:
    """Open the shared client with the Bearer token from the mounted file. Idempotent."""
    global _client
    if _client is not None:
        return
    url, token_file = settings.wikijs_url, settings.wikijs_token_file
    if not url or not token_file:
        raise ValueError("MCP_WIKIJS_URL and MCP_WIKIJS_TOKEN_FILE are required")
    token = Path(token_file).read_text(encoding="utf-8").strip()
    if not token:
        raise ValueError(f"{token_file} is empty")
    _client = httpx.AsyncClient(
        base_url=url.rstrip("/"),
        headers={"Authorization": f"Bearer {token}"},
        timeout=_TIMEOUT_SECONDS,
    )
    log.info("wikijs_client_ready", url=url)


async def close() -> None:
    """Close and drop the shared client."""
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None


def _require_client() -> httpx.AsyncClient:
    if _client is None:
        raise RuntimeError("wiki tools are disabled — set MCP_WIKIJS_URL and MCP_WIKIJS_TOKEN_FILE")
    return _client


async def _graphql(query: str, variables: dict[str, Any] | None = None) -> dict[str, Any]:
    """Run one query and return its ``pages`` payload; GraphQL errors raise WikiError."""
    response = await _require_client().post(
        "/graphql", json={"query": query, "variables": variables or {}}
    )
    response.raise_for_status()
    body: dict[str, Any] = response.json()
    if body.get("errors"):
        messages = "; ".join(str(err["message"]) for err in body["errors"])
        raise WikiError(f"wiki query failed: {messages}")
    pages: dict[str, Any] = body["data"]["pages"]
    return pages


async def _list_entries() -> list[dict[str, Any]]:
    """Every ``pages.list`` row the key may read, ordered by path."""
    entries: list[dict[str, Any]] = (await _graphql(_LIST_QUERY))["list"]
    return entries


async def _source(locale: str, path: str) -> str:
    """Raw source of one page from the ``/s/`` view — the read-only route to content."""
    response = await _require_client().get(f"/s/{locale}/{path}")
    if response.status_code == 403:
        raise WikiError(
            f"the wiki key may not read the source of {path!r} — its group needs read:source"
        )
    response.raise_for_status()
    found = _SOURCE_RE.search(response.text)
    if found is None:
        raise WikiError(
            f"source view of {path!r} has no <code v-pre> block — Wiki.js layout changed?"
        )
    return html.unescape(found.group(1))


def _summary(entry: dict[str, Any]) -> dict[str, Any]:
    """A ``pages.list`` row reduced to the browse view."""
    return {
        "id": entry["id"],
        "path": entry["path"],
        "locale": entry["locale"],
        "title": entry["title"],
        "description": entry["description"],
        "updated_at": entry["updatedAt"],
    }


async def search_wiki(query: str) -> dict[str, Any]:
    """``pages.search``: title/description/path hits among pages the key may read."""
    result = (await _graphql(_SEARCH_QUERY, {"query": query}))["search"]
    return {
        "query": query,
        "total_hits": result["totalHits"],
        "results": [
            {
                "id": int(hit["id"]),  # search engines index the page id as a string
                "path": hit["path"],
                "locale": hit["locale"],
                "title": hit["title"],
                "description": hit["description"],
            }
            for hit in result["results"]
        ],
    }


async def list_wiki_pages() -> dict[str, Any]:
    """``pages.list`` ordered by path — every page the key may read."""
    pages = [_summary(entry) for entry in await _list_entries()]
    return {"count": len(pages), "pages": pages}


async def get_wiki_page(path: str | None = None, page_id: int | None = None) -> dict[str, Any]:
    """One page in full: its list row plus the raw source from the ``/s/`` view.

    The page is resolved through ``pages.list`` first, so the locale the
    source view needs always comes from the page itself.
    """
    if page_id is not None and path is None:
        return await _read_page("id", page_id)
    if path is not None and page_id is None:
        return await _read_page("path", path.strip("/"))
    raise ValueError("pass exactly one of path or page_id")


async def _read_page(field: str, value: object) -> dict[str, Any]:
    """Resolve one list row by ``field == value``, then fetch its source."""
    # pages.list, not pages.single — the latter is guarded by manage:pages.
    ref = f"{field} {value!r}"
    matches = [entry for entry in await _list_entries() if entry[field] == value]
    if not matches:
        raise WikiError(f"no readable wiki page with {ref} — try search_wiki or list_wiki_pages")
    if len(matches) > 1:
        candidates = ", ".join(f"id {entry['id']} ({entry['locale']})" for entry in matches)
        raise WikiError(f"{ref} exists in several locales: {candidates} — call again with page_id")
    entry = matches[0]
    content = await _source(entry["locale"], entry["path"])
    return {
        **_summary(entry),
        "created_at": entry["createdAt"],
        "content_type": entry["contentType"],
        "tags": entry["tags"],
        "content": content,
    }
