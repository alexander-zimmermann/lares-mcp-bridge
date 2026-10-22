"""Wiki-tool tests against a respx-mocked Wiki.js.

Unit tests of the two GraphQL reads, the by-id / by-path resolution through
``pages.list``, the source-view scrape that stands in for the
``manage:pages``-guarded ``pages.single``, and the one write under its own
key. Nothing here needs the database.
"""

from __future__ import annotations

import json
import re
from collections.abc import AsyncIterator, Callable, Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest
import pytest_asyncio
import respx

from lares_mcp_bridge.config import Settings
from lares_mcp_bridge.tools import wiki

from .support import GATEWAY_KEY

WIKI_URL = "http://wiki-js.wiki-js.svc"
TOKEN = "eyJhbGciOiJSUzI1NiJ9.test.token"
WRITE_TOKEN = "eyJhbGciOiJSUzI1NiJ9.write.token"

_LOGIC_BLOCKS = {
    "id": 12,
    "path": "basalte/logic-blocks",
    "locale": "de",
    "title": "Basalte Logic Blocks",
    "description": "Reference of the Studio logic blocks",
    "contentType": "markdown",
    "tags": ["basalte", "reference"],
    "createdAt": "2026-08-01T10:00:00.000Z",
    "updatedAt": "2026-09-01T12:34:56.000Z",
}
_HOME = {
    "id": 1,
    "path": "home",
    "locale": "de",
    "title": "Home",
    "description": "",
    "contentType": "markdown",
    "tags": [],
    "createdAt": "2026-01-01T00:00:00.000Z",
    "updatedAt": "2026-01-02T00:00:00.000Z",
}


def _settings(
    wikijs_url: str | None = None,
    wikijs_token_file: str | None = None,
    wikijs_write_token_file: str | None = None,
) -> Settings:
    return Settings(
        db_host="localhost",
        db_name="x",
        db_username="x",
        db_password="x",
        gateway_key=GATEWAY_KEY,
        nats_enabled=False,
        wikijs_url=wikijs_url,
        wikijs_token_file=wikijs_token_file,
        wikijs_write_token_file=wikijs_write_token_file,
    )


def _graphql(pages: dict[str, Any]) -> httpx.Response:
    return httpx.Response(200, json={"data": {"pages": pages}})


def _source_view(content: str) -> httpx.Response:
    """The ``/s/`` page as Wiki.js renders it: source escaped inside ``<code v-pre>``."""
    escaped = content.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    body = (
        "<!DOCTYPE html><html><body><div id='root'>"
        "<page-source :page-id='12' locale='de' path='basalte/logic-blocks'>"
        f"<code v-pre>{escaped}</code></page-source></div></body></html>"
    )
    return httpx.Response(200, text=body)


@pytest.fixture
def token_file(tmp_path: Path) -> Path:
    path = tmp_path / "wikijs-token"
    path.write_text(f"{TOKEN}\n", encoding="utf-8")  # trailing newline, like a mounted Secret
    return path


@pytest_asyncio.fixture
async def wiki_client(token_file: Path) -> AsyncIterator[None]:
    await wiki.init(_settings(wikijs_url=WIKI_URL, wikijs_token_file=str(token_file)))
    try:
        yield
    finally:
        await wiki.close()


@pytest.fixture
def write_token_file(tmp_path: Path) -> Path:
    path = tmp_path / "wikijs-write-token"
    path.write_text(f"{WRITE_TOKEN}\n", encoding="utf-8")
    return path


@pytest_asyncio.fixture
async def wiki_writer(token_file: Path, write_token_file: Path) -> AsyncIterator[None]:
    await wiki.init_writer(
        _settings(
            wikijs_url=WIKI_URL,
            wikijs_token_file=str(token_file),
            wikijs_write_token_file=str(write_token_file),
        )
    )
    try:
        yield
    finally:
        await wiki.close()


@pytest.fixture
def router() -> Iterator[respx.MockRouter]:
    with respx.mock(base_url=WIKI_URL, assert_all_called=False) as mock:
        yield mock


def _last_graphql(router: respx.MockRouter) -> dict[str, Any]:
    request = router.calls.last.request
    body: dict[str, Any] = json.loads(request.content)
    return body


# ---------------------------------------------------------------- settings


def test_wikijs_settings_must_be_set_together(token_file: Path) -> None:
    with pytest.raises(ValueError, match="MCP_WIKIJS_URL and MCP_WIKIJS_TOKEN_FILE"):
        _settings(wikijs_url=WIKI_URL)
    with pytest.raises(ValueError, match="MCP_WIKIJS_URL and MCP_WIKIJS_TOKEN_FILE"):
        _settings(wikijs_token_file=str(token_file))


def test_wikijs_enabled_only_when_both_set(token_file: Path) -> None:
    assert _settings().wikijs_enabled is False
    assert _settings(wikijs_url=WIKI_URL, wikijs_token_file=str(token_file)).wikijs_enabled


# ---------------------------------------------------------------- lifecycle


async def test_tools_refuse_when_not_initialised() -> None:
    with pytest.raises(RuntimeError, match="MCP_WIKIJS_URL"):
        await wiki.list_wiki_pages()


async def test_init_rejects_empty_token_file(tmp_path: Path) -> None:
    empty = tmp_path / "wikijs-token"
    empty.write_text("\n", encoding="utf-8")
    with pytest.raises(ValueError, match="empty"):
        await wiki.init(_settings(wikijs_url=WIKI_URL, wikijs_token_file=str(empty)))


async def test_init_refuses_without_wiki_settings() -> None:
    with pytest.raises(ValueError, match="MCP_WIKIJS_URL"):
        await wiki.init(_settings())


# ---------------------------------------------------------------- search_wiki


async def test_search_wiki_maps_hits_and_sends_bearer(
    wiki_client: None, router: respx.MockRouter
) -> None:
    router.post("/graphql").mock(
        return_value=_graphql(
            {
                "search": {
                    "totalHits": 1,
                    "results": [
                        {
                            "id": "12",
                            "path": "basalte/logic-blocks",
                            "locale": "de",
                            "title": "Basalte Logic Blocks",
                            "description": "Reference of the Studio logic blocks",
                        }
                    ],
                }
            }
        )
    )

    out = await wiki.search_wiki("logic")

    assert out == {
        "query": "logic",
        "total_hits": 1,
        "results": [
            {
                "id": 12,
                "path": "basalte/logic-blocks",
                "locale": "de",
                "title": "Basalte Logic Blocks",
                "description": "Reference of the Studio logic blocks",
            }
        ],
    }
    request = router.calls.last.request
    assert request.headers["authorization"] == f"Bearer {TOKEN}"
    body = _last_graphql(router)
    assert body["variables"] == {"query": "logic"}
    assert "search(query: $query)" in body["query"]


# ---------------------------------------------------------------- list_wiki_pages


async def test_list_wiki_pages_maps_rows(wiki_client: None, router: respx.MockRouter) -> None:
    router.post("/graphql").mock(return_value=_graphql({"list": [_LOGIC_BLOCKS, _HOME]}))

    out = await wiki.list_wiki_pages()

    assert out["count"] == 2
    assert out["pages"][0] == {
        "id": 12,
        "path": "basalte/logic-blocks",
        "locale": "de",
        "title": "Basalte Logic Blocks",
        "description": "Reference of the Studio logic blocks",
        "updated_at": "2026-09-01T12:34:56.000Z",
    }
    assert "list(orderBy: PATH)" in _last_graphql(router)["query"]


# ---------------------------------------------------------------- get_wiki_page


async def test_get_wiki_page_by_id_reads_source_view(
    wiki_client: None, router: respx.MockRouter
) -> None:
    router.post("/graphql").mock(return_value=_graphql({"list": [_HOME, _LOGIC_BLOCKS]}))
    source = router.get("/s/de/basalte/logic-blocks").mock(
        return_value=_source_view("# Logic blocks\n\nUse `<AND>` & `<OR>` blocks.\n")
    )

    out = await wiki.get_wiki_page(page_id=12)

    assert source.called  # locale and path come from the page's own list row
    assert out == {
        "id": 12,
        "path": "basalte/logic-blocks",
        "locale": "de",
        "title": "Basalte Logic Blocks",
        "description": "Reference of the Studio logic blocks",
        "updated_at": "2026-09-01T12:34:56.000Z",
        "created_at": "2026-08-01T10:00:00.000Z",
        "content_type": "markdown",
        "tags": ["basalte", "reference"],
        "content": "# Logic blocks\n\nUse `<AND>` & `<OR>` blocks.\n",
    }


async def test_get_wiki_page_by_path_strips_slashes(
    wiki_client: None, router: respx.MockRouter
) -> None:
    router.post("/graphql").mock(return_value=_graphql({"list": [_HOME, _LOGIC_BLOCKS]}))
    source = router.get("/s/de/basalte/logic-blocks").mock(return_value=_source_view("hello"))

    out = await wiki.get_wiki_page(path="/basalte/logic-blocks/")

    assert source.called
    assert out["id"] == 12
    assert out["content"] == "hello"


async def test_get_wiki_page_ambiguous_locales_name_candidates(
    wiki_client: None, router: respx.MockRouter
) -> None:
    english = {**_LOGIC_BLOCKS, "id": 13, "locale": "en"}
    router.post("/graphql").mock(return_value=_graphql({"list": [_LOGIC_BLOCKS, english]}))

    with pytest.raises(wiki.WikiError, match=r"id 12 \(de\).*id 13 \(en\).*page_id"):
        await wiki.get_wiki_page(path="basalte/logic-blocks")


async def test_get_wiki_page_unknown_path(wiki_client: None, router: respx.MockRouter) -> None:
    router.post("/graphql").mock(return_value=_graphql({"list": [_HOME]}))

    with pytest.raises(wiki.WikiError, match="no readable wiki page with path 'nope'"):
        await wiki.get_wiki_page(path="nope")


async def test_get_wiki_page_unknown_id(wiki_client: None, router: respx.MockRouter) -> None:
    router.post("/graphql").mock(return_value=_graphql({"list": [_HOME]}))

    with pytest.raises(wiki.WikiError, match="no readable wiki page with id 99"):
        await wiki.get_wiki_page(page_id=99)


async def test_get_wiki_page_needs_exactly_one_selector(wiki_client: None) -> None:
    with pytest.raises(ValueError, match="exactly one of path or page_id"):
        await wiki.get_wiki_page()
    with pytest.raises(ValueError, match="exactly one of path or page_id"):
        await wiki.get_wiki_page(path="home", page_id=1)


async def test_get_wiki_page_forbidden_source_names_permission(
    wiki_client: None, router: respx.MockRouter
) -> None:
    router.post("/graphql").mock(return_value=_graphql({"list": [_LOGIC_BLOCKS]}))
    router.get("/s/de/basalte/logic-blocks").mock(
        return_value=httpx.Response(403, text="<html>Unauthorized</html>")
    )

    with pytest.raises(wiki.WikiError, match="read:source"):
        await wiki.get_wiki_page(page_id=12)


async def test_get_wiki_page_unexpected_markup(wiki_client: None, router: respx.MockRouter) -> None:
    router.post("/graphql").mock(return_value=_graphql({"list": [_LOGIC_BLOCKS]}))
    router.get("/s/de/basalte/logic-blocks").mock(
        return_value=httpx.Response(200, text="<html><body>no source here</body></html>")
    )

    with pytest.raises(wiki.WikiError, match="code v-pre"):
        await wiki.get_wiki_page(page_id=12)


# ---------------------------------------------------------------- transport errors


async def test_graphql_errors_raise_wiki_error(wiki_client: None, router: respx.MockRouter) -> None:
    router.post("/graphql").mock(
        return_value=httpx.Response(
            200,
            json={
                "errors": [{"message": "Forbidden", "path": ["pages", "list"]}],
                "data": {"pages": {"list": None}},
            },
        )
    )

    with pytest.raises(wiki.WikiError, match="Forbidden"):
        await wiki.list_wiki_pages()


async def test_http_errors_propagate(wiki_client: None, router: respx.MockRouter) -> None:
    router.post("/graphql").mock(return_value=httpx.Response(502, text="bad gateway"))

    with pytest.raises(httpx.HTTPStatusError):
        await wiki.search_wiki("anything")


# ---------------------------------------------------------------- update_wiki_page


def _write_list(*entries: dict[str, Any]) -> httpx.Response:
    """``pages.list`` as the write key sees it, with the fields an update carries over."""
    return _graphql({"list": list(entries)})


def _wiki(
    listed: httpx.Response, action: str, row: dict[str, Any]
) -> Callable[[httpx.Request], httpx.Response]:
    """Wiki.js answering the lookup with ``listed`` and the write with ``row``.

    ``row`` is the raw page row ``createPage``/``updatePage`` return: it carries
    ``localeCode``, never ``locale``. A selected field the row lacks fails as
    Wiki.js fails it — the write stands, the answer is an error.
    """

    def answer(request: httpx.Request) -> httpx.Response:
        query = json.loads(request.content)["query"]
        if "list(" in query:
            return listed
        selection = re.search(r"page \{([^}]*)\}", query)
        assert selection is not None
        missing = [field for field in selection.group(1).split() if field not in row]
        status = {"succeeded": True, "errorCode": 0, "slug": "ok", "message": "ok"}
        if missing:
            errors = [
                {"message": f"Cannot return null for non-nullable field Page.{field}."}
                for field in missing
            ]
            result = {"responseResult": status, "page": None}
            return httpx.Response(200, json={"errors": errors, "data": {"pages": {action: result}}})
        page = {field: row[field] for field in selection.group(1).split()}
        return httpx.Response(
            200, json={"data": {"pages": {action: {"responseResult": status, "page": page}}}}
        )

    return answer


def _row(page_id: int, path: str, title: str, updated_at: str) -> dict[str, Any]:
    return {
        "id": page_id,
        "path": path,
        "title": title,
        "localeCode": "en",
        "updatedAt": updated_at,
    }


def _graphql_bodies(router: respx.MockRouter) -> list[dict[str, Any]]:
    return [json.loads(call.request.content) for call in router.calls]


def test_the_write_key_needs_the_wiki_url(write_token_file: Path) -> None:
    with pytest.raises(ValueError, match="MCP_WIKIJS_WRITE_TOKEN_FILE needs MCP_WIKIJS_URL"):
        _settings(wikijs_write_token_file=str(write_token_file))


def test_writes_enabled_only_with_the_write_key(token_file: Path, write_token_file: Path) -> None:
    reads = _settings(wikijs_url=WIKI_URL, wikijs_token_file=str(token_file))
    assert reads.wikijs_write_enabled is False
    writes = _settings(
        wikijs_url=WIKI_URL,
        wikijs_token_file=str(token_file),
        wikijs_write_token_file=str(write_token_file),
    )
    assert writes.wikijs_write_enabled


async def test_writes_refuse_when_not_initialised(wiki_client: None) -> None:
    with pytest.raises(RuntimeError, match="MCP_WIKIJS_WRITE_TOKEN_FILE"):
        await wiki.update_wiki_page("haus/wartungsplan", "# Plan", "Wartungsplan")


async def test_a_new_path_is_created_with_the_write_key(
    wiki_writer: None, router: respx.MockRouter
) -> None:
    row = _row(31, "haus/wartungsplan", "Wartungsplan", "2026-10-03T08:00:00.000Z")
    router.post("/graphql").mock(side_effect=_wiki(_write_list(), "create", row))

    out = await wiki.update_wiki_page("/haus/wartungsplan/", "# Plan\n", "Wartungsplan")

    assert out == {
        "action": "created",
        "id": 31,
        "path": "haus/wartungsplan",
        "locale": "en",
        "title": "Wartungsplan",
        "updated_at": "2026-10-03T08:00:00.000Z",
    }
    assert all(
        call.request.headers["authorization"] == f"Bearer {WRITE_TOKEN}" for call in router.calls
    )
    listed, created = _graphql_bodies(router)
    # The page is looked up in the one locale writes go to.
    assert listed["variables"] == {"locale": "en"}
    assert "create(" in created["query"]
    assert created["variables"] == {
        "content": "# Plan\n",
        "locale": "en",
        "path": "haus/wartungsplan",
        "title": "Wartungsplan",
    }


async def test_an_existing_page_keeps_description_tags_and_publication(
    wiki_writer: None, router: respx.MockRouter
) -> None:
    existing = {
        "id": 12,
        "path": "basalte/logic-blocks",
        "locale": "en",
        "title": "Basalte Logic Blocks",
        "description": "Reference of the Studio logic blocks",
        "isPublished": True,
        "tags": ["basalte", "reference"],
    }
    other_page = {**existing, "id": 1, "path": "home", "tags": []}
    row = _row(12, "basalte/logic-blocks", "Logikblöcke", "2026-10-03T08:05:00.000Z")
    router.post("/graphql").mock(
        side_effect=_wiki(_write_list(other_page, existing), "update", row)
    )

    out = await wiki.update_wiki_page("basalte/logic-blocks", "# Neu", "Logikblöcke")

    assert out["action"] == "updated"
    assert out["id"] == 12
    assert out["title"] == "Logikblöcke"
    _, updated = _graphql_bodies(router)
    assert "update(" in updated["query"]
    # Wiki.js resets what an update leaves out: an omitted publish flag
    # unpublishes the page, omitted tags break the update.
    assert updated["variables"] == {
        "id": 12,
        "content": "# Neu",
        "title": "Logikblöcke",
        "description": "Reference of the Studio logic blocks",
        "isPublished": True,
        "tags": ["basalte", "reference"],
    }


async def test_a_refused_write_raises_with_the_wikis_reason(
    wiki_writer: None, router: respx.MockRouter
) -> None:
    refused = {
        "responseResult": {
            "succeeded": False,
            "errorCode": 6010,
            "slug": "PageDeleteForbidden",
            "message": "You are not authorized to delete this page.",
        },
        "page": None,
    }
    router.post("/graphql").mock(
        side_effect=[
            _write_list(),
            httpx.Response(200, json={"data": {"pages": {"create": refused}}}),
        ]
    )

    with pytest.raises(wiki.WikiError, match="PageDeleteForbidden.*not authorized"):
        await wiki.update_wiki_page("system/secrets", "x", "x")


async def test_a_key_without_write_rights_is_forbidden(
    wiki_writer: None, router: respx.MockRouter
) -> None:
    router.post("/graphql").mock(
        side_effect=[
            _write_list(),
            httpx.Response(
                200,
                json={
                    "errors": [{"message": "Forbidden", "path": ["pages", "create"]}],
                    "data": {"pages": {"create": None}},
                },
            ),
        ]
    )

    with pytest.raises(wiki.WikiError, match="Forbidden"):
        await wiki.update_wiki_page("haus/wartungsplan", "# Plan", "Wartungsplan")


async def test_a_page_without_a_description_keeps_none(
    wiki_writer: None, router: respx.MockRouter
) -> None:
    existing = {
        "id": 7,
        "path": "haus/notizen",
        "locale": "en",
        "description": None,
        "isPublished": False,
        "tags": [],
    }
    row = _row(7, "haus/notizen", "Notizen", "2026-10-03T08:10:00.000Z")
    router.post("/graphql").mock(side_effect=_wiki(_write_list(existing), "update", row))

    out = await wiki.update_wiki_page("haus/notizen", "# Notizen", "Notizen")

    assert out["action"] == "updated"
    _, updated = _graphql_bodies(router)
    # Carried over as it is, a draft stays a draft.
    assert updated["variables"]["description"] is None
    assert updated["variables"]["isPublished"] is False
