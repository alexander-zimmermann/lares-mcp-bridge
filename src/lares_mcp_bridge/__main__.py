"""Console entry point — runs the MCP server app under uvicorn."""

from __future__ import annotations

import uvicorn

from .config import load_settings


def main() -> None:
    """Load settings and serve ``build_app()`` (factory keeps import side-effect free)."""
    settings = load_settings()
    uvicorn.run(
        "lares_mcp_bridge.server:build_app",
        factory=True,
        host=settings.host,
        port=settings.port,
        log_config=None,
    )


if __name__ == "__main__":
    main()
