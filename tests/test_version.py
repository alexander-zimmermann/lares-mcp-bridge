"""The version the MCP handshake reports is the project version.

``__version__`` reads the installed distribution rather than carrying its own
copy, so this pins both ends: that the package version tracks pyproject, and
that it reaches the handshake clients see.
"""

import tomllib
from pathlib import Path

from lares_mcp_bridge import server

PYPROJECT = Path(__file__).resolve().parent.parent / "pyproject.toml"


def test_server_reports_project_version() -> None:
    project_version = tomllib.loads(PYPROJECT.read_text())["project"]["version"]
    assert server.mcp.version == project_version
