"""Bridges LangGraph MCP tools into the synchronous service layer."""

from __future__ import annotations

import threading
from collections.abc import Mapping
from typing import Any

from anyio.from_thread import BlockingPortal, start_blocking_portal
from langchain_core.tools import BaseTool
from langchain_mcp_adapters.client import MultiServerMCPClient

from tiangong_lca_spec.core.config import Settings, get_settings
from tiangong_lca_spec.core.exceptions import SpecCodingError
from tiangong_lca_spec.core.json_utils import parse_json_response
from tiangong_lca_spec.core.logging import get_logger

LOGGER = get_logger(__name__)


class MCPToolClient:
    """Synchronously accessible wrapper around ``MultiServerMCPClient``.

    The underlying MCP utilities are asynchronous. This class hides the async
    details behind a shared ``BlockingPortal`` so the rest of the codebase can
    remain synchronous.
    """

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        connections: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> None:
        self._settings = settings or get_settings()
        connection_map = (
            dict(connections) if connections is not None else self._settings.mcp_service_configs()
        )
        self._client = MultiServerMCPClient(connection_map)
        self._portal_cm = start_blocking_portal()
        self._portal: BlockingPortal = self._portal_cm.__enter__()
        self._tools: dict[tuple[str, str], BaseTool] = {}
        self._lock = threading.RLock()
        self._closed = False
        LOGGER.debug(
            "mcp_tool_client.initialized",
            servers=list(connection_map.keys()),
        )

    # Public API -----------------------------------------------------------------

    def invoke_tool(
        self,
        server_name: str,
        tool_name: str,
        arguments: Mapping[str, Any] | None = None,
    ) -> tuple[str | list[str], Any]:
        """Call a remote MCP tool and return its raw result.

        Returns the text payload (string or list of strings) together with
        any non-text attachments supplied by the MCP adapter.
        """
        if self._closed:
            raise RuntimeError("Cannot invoke MCP tool on a closed client")
        tool = self._get_tool(server_name, tool_name)
        payload = arguments or {}
        LOGGER.debug(
            "mcp_tool_client.invoke",
            server=server_name,
            tool=tool_name,
            keys=list(payload.keys()),
        )
        result = self._portal.call(tool.ainvoke, dict(payload))
        if isinstance(result, tuple) and len(result) == 2:
            return result
        return result, None

    def invoke_json_tool(
        self,
        server_name: str,
        tool_name: str,
        arguments: Mapping[str, Any] | None = None,
    ) -> Any:
        """Invoke a tool and parse its textual response as JSON."""
        text_content, _ = self.invoke_tool(server_name, tool_name, arguments)
        raw_text = self._coerce_text(text_content)
        if not raw_text:
            return None
        try:
            return parse_json_response(raw_text)
        except SpecCodingError:
            LOGGER.error(
                "mcp_tool_client.json_parse_failed",
                server=server_name,
                tool=tool_name,
                preview=raw_text[:200],
            )
            raise
        finally:
            LOGGER.debug(
                "mcp_tool_client.invoke.completed",
                server=server_name,
                tool=tool_name,
            )

    def close(self) -> None:
        """Shut down the portal and release cached tool handles."""
        if self._closed:
            return
        self._closed = True
        self._tools.clear()
        try:
            self._portal_cm.__exit__(None, None, None)
        finally:
            LOGGER.debug("mcp_tool_client.closed")

    def __enter__(self) -> "MCPToolClient":
        if self._closed:
            raise RuntimeError("Cannot re-enter a closed MCPToolClient")
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    # Internal helpers -----------------------------------------------------------

    def _get_tool(self, server_name: str, tool_name: str) -> BaseTool:
        cache_key = (server_name, tool_name)
        with self._lock:
            tool = self._tools.get(cache_key)
            if tool is not None:
                return tool
            tool = self._portal.call(self._load_tool, server_name, tool_name)
            self._tools[cache_key] = tool
            return tool

    async def _load_tool(self, server_name: str, tool_name: str) -> BaseTool:
        tools = await self._client.get_tools(server_name=server_name)
        for tool in tools:
            if tool.name == tool_name:
                return tool
        msg = f"MCP tool '{tool_name}' not found on server '{server_name}'"
        raise SpecCodingError(msg)

    @staticmethod
    def _coerce_text(content: str | list[str] | None) -> str:
        if content is None:
            return ""
        if isinstance(content, list):
            return "\n".join(piece for piece in content if piece)
        return str(content)


__all__ = ["MCPToolClient"]
