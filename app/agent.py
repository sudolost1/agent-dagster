from __future__ import annotations

import asyncio
import json
import logging
from typing import TypeVar

from openai import OpenAI
from pydantic import BaseModel

from mcp.types import Implementation

from app.config import get_config, get_editable
from app.context import build_prefix

T = TypeVar("T", bound=BaseModel)
log = logging.getLogger(__name__)


class Agent:
    """Base wrapper around the OpenAI SDK with MCP tool-calling support.

    Reads connection / prompt defaults from the editable config so they can be
    changed at runtime via the API.  MCP tool servers are discovered from the
    ``mcp_tools`` config list and connected to via Docker hostnames.
    """

    MCP_PATH = "/mcp"

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str | None = None,
        model: str | None = None,
        system_prompt: str | None = None,
    ):
        self._api_key = api_key
        self._base_url_override = base_url
        self._model_override = model
        self._system_prompt_override = system_prompt

    def _client(self) -> OpenAI:
        cfg = get_editable()
        return OpenAI(
            api_key=self._api_key,
            base_url=self._base_url_override or cfg.get("base_url"),
        )

    @property
    def model(self) -> str:
        if self._model_override:
            return self._model_override
        return get_editable().get("model", "gpt-4o")

    @property
    def system_prompt(self) -> str:
        if self._system_prompt_override:
            return self._system_prompt_override
        return get_editable().get("prompt", "You are a helpful assistant.")

    # -- MCP helpers ----------------------------------------------------------

    @classmethod
    def _gen_tool_url(cls, service_name: str, port: int = 8001) -> str:
        """Build the MCP streamable-HTTP endpoint URL from a Docker service name.

        In Docker Compose the service name *is* the hostname, so
        ``_gen_tool_url("mcp-base", 8001)`` → ``http://mcp-base:8001/mcp``.
        """
        return f"http://{service_name}:{port}{cls.MCP_PATH}"

    # -- Structured-output (existing) -----------------------------------------

    def parse(
        self,
        user_message: str,
        response_format: type[T],
        *,
        model: str | None = None,
        system_prompt: str | None = None,
    ) -> T:
        """Call the OpenAI beta structured-output parse endpoint.

        Returns a validated Pydantic model instance of ``response_format``.
        Context from subscribed Redis streams is prepended to the user message.
        """
        prefix = build_prefix()
        augmented = prefix + user_message if prefix else user_message
        client = self._client()
        completion = client.beta.chat.completions.parse(
            model=model or self.model,
            messages=[
                {"role": "system", "content": system_prompt or self.system_prompt},
                {"role": "user", "content": augmented},
            ],
            response_format=response_format,
        )
        result = completion.choices[0].message.parsed
        if result is None:
            raise ValueError("Model returned a refusal or unparseable response")
        return result

    # -- Tool-calling agent loop ----------------------------------------------

    def run(
        self,
        user_message: str,
        *,
        model: str | None = None,
        system_prompt: str | None = None,
        max_iterations: int = 10,
    ) -> str:
        """Run an agentic loop: chat with the LLM, calling MCP tools as needed.

        Returns the final assistant text response.
        """
        return asyncio.run(
            self._arun(
                user_message,
                model=model,
                system_prompt=system_prompt,
                max_iterations=max_iterations,
            )
        )

    def _mcp_client_info(self) -> Implementation:
        """Return an MCP Implementation identifying this agent to tool servers."""
        cfg = get_editable()
        name = cfg.get("agent-name", "agent-base")
        version = get_config().get("static", {}).get("version", "0.0.0")
        return Implementation(name=name, version=version)

    async def _arun(
        self,
        user_message: str,
        *,
        model: str | None = None,
        system_prompt: str | None = None,
        max_iterations: int = 10,
    ) -> str:
        from fastmcp import Client

        prefix = build_prefix()
        augmented = prefix + user_message if prefix else user_message

        mcp_entries: list[dict] = get_editable().get("mcp_tools", [])
        client_info = self._mcp_client_info()

        openai_tools: list[dict] = []
        tool_map: dict[str, tuple[str, str]] = {}

        for entry in mcp_entries:
            url = self._gen_tool_url(entry["name"], entry.get("port", 8001))
            try:
                async with Client(url, client_info=client_info) as mcp_client:
                    discovered = await mcp_client.list_tools()
                    for tool in discovered:
                        qualified = f"{entry['name']}_{tool.name}".replace("-", "_")
                        tool_map[qualified] = (url, tool.name)
                        openai_tools.append({
                            "type": "function",
                            "function": {
                                "name": qualified,
                                "description": tool.description or "",
                                "parameters": tool.inputSchema,
                            },
                        })
                log.info("Discovered %d tool(s) from %s", len(discovered), url)
            except Exception:
                log.warning("MCP server %s unavailable, skipping", url, exc_info=True)

        messages: list[dict] = [
            {"role": "system", "content": system_prompt or self.system_prompt},
            {"role": "user", "content": augmented},
        ]

        llm = self._client()

        for _ in range(max_iterations):
            kwargs: dict = {"model": model or self.model, "messages": messages}
            if openai_tools:
                kwargs["tools"] = openai_tools

            response = llm.chat.completions.create(**kwargs)
            choice = response.choices[0]
            assistant_msg = choice.message

            if not assistant_msg.tool_calls:
                return assistant_msg.content or ""

            msg_dict: dict = {"role": "assistant", "content": assistant_msg.content}
            msg_dict["tool_calls"] = [
                {
                    "id": tc.id,
                    "type": "function",
                    "function": {
                        "name": tc.function.name,
                        "arguments": tc.function.arguments,
                    },
                }
                for tc in assistant_msg.tool_calls
            ]
            messages.append(msg_dict)

            for tc in assistant_msg.tool_calls:
                fn_name = tc.function.name
                if fn_name not in tool_map:
                    messages.append({
                        "role": "tool",
                        "tool_call_id": tc.id,
                        "content": f"Error: unknown tool '{fn_name}'",
                    })
                    continue

                server_url, orig_name = tool_map[fn_name]
                arguments = json.loads(tc.function.arguments)
                log.info("Calling MCP tool %s(%s) on %s", orig_name, arguments, server_url)

                try:
                    async with Client(server_url, client_info=client_info) as mcp_client:
                        result = await mcp_client.call_tool(orig_name, arguments)
                    data = result.data if hasattr(result, "data") else result
                    tool_output = json.dumps(data) if isinstance(data, (dict, list)) else str(data)
                except Exception as exc:
                    log.exception("Tool call %s failed", orig_name)
                    tool_output = f"Error: {exc}"

                messages.append({
                    "role": "tool",
                    "tool_call_id": tc.id,
                    "content": tool_output,
                })

        log.warning("Max iterations (%d) reached without final response", max_iterations)
        last = messages[-1]
        return last.get("content", "") if isinstance(last, dict) else ""
