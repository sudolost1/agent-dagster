from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import TypeVar

from openai import OpenAI
from pydantic import BaseModel

from mcp.types import Implementation

from app.config import get_config, get_editable
from app.context import build_prefix_with_ids, get_new_entries
from app.tools import LOCAL_TOOLS, LOCAL_TOOL_SCHEMAS
from app import buildlog

T = TypeVar("T", bound=BaseModel)
log = logging.getLogger(__name__)


class Agent:
    """Dagster pipeline builder agent.

    Extends the base agent with:
    - Local tools (test_connection, write_pipeline, launch_run, get_run_status)
    - Timeout-based loop (no hard iteration cap)
    - Per-iteration context refresh from Redis streams
    - Step-level logging to Redis via buildlog
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

    @classmethod
    def _gen_tool_url(cls, service_name: str, port: int = 8001) -> str:
        return f"http://{service_name}:{port}{cls.MCP_PATH}"

    def parse(
        self,
        user_message: str,
        response_format: type[T],
        *,
        model: str | None = None,
        system_prompt: str | None = None,
    ) -> T:
        from app.context import build_prefix
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

    def _mcp_client_info(self) -> Implementation:
        cfg = get_editable()
        name = cfg.get("agent-name", "agent-dagster")
        version = get_config().get("static", {}).get("version", "0.0.0")
        return Implementation(name=name, version=version)

    # ------------------------------------------------------------------
    # Extended build loop
    # ------------------------------------------------------------------

    def run(
        self,
        user_message: str,
        *,
        build_id: str | None = None,
        project_name: str = "",
        job_name: str = "",
        model: str | None = None,
        system_prompt: str | None = None,
    ) -> str:
        return asyncio.run(
            self._arun(
                user_message,
                build_id=build_id,
                project_name=project_name,
                job_name=job_name,
                model=model,
                system_prompt=system_prompt,
            )
        )

    async def _arun(
        self,
        user_message: str,
        *,
        build_id: str | None = None,
        project_name: str = "",
        job_name: str = "",
        model: str | None = None,
        system_prompt: str | None = None,
    ) -> str:
        from fastmcp import Client

        cfg = get_editable()
        timeout = cfg.get("build_timeout_seconds", 600)
        deadline = time.monotonic() + timeout

        prefix, last_ids = build_prefix_with_ids()
        augmented = prefix + user_message if prefix else user_message

        # -- discover MCP tools ------------------------------------------------
        mcp_entries: list[dict] = cfg.get("mcp_tools", [])
        client_info = self._mcp_client_info()

        openai_tools: list[dict] = list(LOCAL_TOOL_SCHEMAS)
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

        # -- build initial messages --------------------------------------------
        messages: list[dict] = [
            {"role": "system", "content": system_prompt or self.system_prompt},
            {"role": "user", "content": augmented},
        ]

        llm = self._client()
        iteration = 0

        while True:
            # check timeout
            if time.monotonic() > deadline:
                log.warning("Build timeout after %ds", timeout)
                if build_id:
                    buildlog.log_step(build_id, project_name, job_name, "build_timeout",
                                      f"Build timed out after {timeout}s")
                return json.dumps({
                    "status": "failed",
                    "summary": f"Build timed out after {timeout} seconds",
                })

            # refresh context each iteration
            if iteration > 0:
                new_entries, last_ids = get_new_entries(last_ids)
                if new_entries:
                    ctx_lines = [f"[{e['stream']}] {e['content']}" for e in new_entries]
                    ctx_msg = "--- New Context ---\n" + "\n".join(ctx_lines) + "\n--- End New Context ---"
                    messages.append({"role": "system", "content": ctx_msg})
                    log.info("Injected %d new context entries at iteration %d", len(new_entries), iteration)
                    if build_id:
                        buildlog.log_step(build_id, project_name, job_name, "context_update", ctx_msg)

            iteration += 1

            # LLM call
            kwargs: dict = {"model": model or self.model, "messages": messages}
            if openai_tools:
                kwargs["tools"] = openai_tools

            response = llm.chat.completions.create(**kwargs)
            choice = response.choices[0]
            assistant_msg = choice.message

            # log the LLM response
            if build_id and assistant_msg.content:
                buildlog.log_step(build_id, project_name, job_name, "llm_response",
                                  assistant_msg.content[:4000])

            # no tool calls = final answer
            if not assistant_msg.tool_calls:
                return assistant_msg.content or ""

            # append assistant message with tool calls
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

            # dispatch each tool call
            for tc in assistant_msg.tool_calls:
                fn_name = tc.function.name
                arguments = json.loads(tc.function.arguments)

                if build_id:
                    buildlog.log_step(build_id, project_name, job_name, "tool_call",
                                      json.dumps({"tool": fn_name, "args": arguments})[:4000])

                # local tool
                if fn_name in LOCAL_TOOLS:
                    log.info("Calling local tool %s(%s)", fn_name, arguments)
                    try:
                        tool_output = LOCAL_TOOLS[fn_name](**arguments)
                    except Exception as exc:
                        log.exception("Local tool %s failed", fn_name)
                        tool_output = json.dumps({"error": str(exc)})

                # MCP tool
                elif fn_name in tool_map:
                    server_url, orig_name = tool_map[fn_name]
                    log.info("Calling MCP tool %s(%s) on %s", orig_name, arguments, server_url)
                    try:
                        async with Client(server_url, client_info=client_info) as mcp_client:
                            result = await mcp_client.call_tool(orig_name, arguments)
                        data = result.data if hasattr(result, "data") else result
                        tool_output = json.dumps(data) if isinstance(data, (dict, list)) else str(data)
                    except Exception as exc:
                        log.exception("MCP tool %s failed", orig_name)
                        tool_output = json.dumps({"error": str(exc)})

                else:
                    tool_output = json.dumps({"error": f"Unknown tool '{fn_name}'"})

                if build_id:
                    buildlog.log_step(build_id, project_name, job_name, "tool_result",
                                      tool_output[:4000])

                messages.append({
                    "role": "tool",
                    "tool_call_id": tc.id,
                    "content": tool_output,
                })
