"""Dagster pipeline builder agent. Structured output parsing + write→validate→materialize loop using Dagster tools."""

from __future__ import annotations

import asyncio
import json
import logging
import time

from openai import OpenAI

from app.config import get_editable
from app.context import build_prefix
from app.schema import AgentOutput, JobStatus
from app import buildlog

log = logging.getLogger(__name__)


class Agent:
    """Orchestrates pipeline generation via LLM structured output and Dagster validation runs."""

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str | None = None,
        model: str | None = None,
        system_prompt: str | None = None,
    ):
        """Initialize with optional LLM config overrides."""
        self._api_key = api_key
        self._base_url_override = base_url
        self._model_override = model
        self._system_prompt_override = system_prompt

    def _client(self) -> OpenAI:
        """Create OpenAI client from current config."""
        cfg = get_editable()
        return OpenAI(
            api_key=self._api_key,
            base_url=self._base_url_override or cfg.get("base_url"),
        )

    @property
    def model(self) -> str:
        """Active model name from override or config."""
        if self._model_override:
            return self._model_override
        return get_editable().get("model", "gpt-4o")

    @property
    def system_prompt(self) -> str:
        """Active system prompt from override or config."""
        if self._system_prompt_override:
            return self._system_prompt_override
        return get_editable().get("prompt", "You are a helpful assistant.")

    def _parse(
        self,
        user_message: str,
        *,
        model: str | None = None,
        system_prompt: str | None = None,
        extra_context: str | None = None,
    ) -> AgentOutput:
        """Call LLM with structured output parsing to get AgentOutput."""
        from app.context import build_prefix

        prefix = build_prefix()
        augmented = prefix + user_message if prefix else user_message
        if extra_context:
            augmented += "\n\n--- Additional Context ---\n" + extra_context + "\n--- End ---"

        client = self._client()
        completion = client.beta.chat.completions.parse(
            model=model or self.model,
            messages=[
                {"role": "system", "content": system_prompt or self.system_prompt},
                {"role": "user", "content": augmented},
            ],
            response_format=AgentOutput,
        )
        result = completion.choices[0].message.parsed
        if result is None:
            raise ValueError("Model returned a refusal or unparseable response")
        return result

    def run(
        self,
        user_message: str,
        *,
        build_id: str | None = None,
        project_name: str = "",
        job_name: str = "",
        model: str | None = None,
        system_prompt: str | None = None,
    ) -> dict:
        """Sync wrapper for the async agent loop. Returns result dict with status, summary, items."""
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
    ) -> dict:
        """Async agent loop: parse→write→validate→materialize→re-prompt, up to 3 iterations."""
        from app.tools import (
            get_run_status,
            launch_run,
            write_pipeline,
        )

        cfg = get_editable()
        timeout = cfg.get("build_timeout_seconds", 600)
        deadline = time.monotonic() + timeout

        log.info(
            "Build %s: starting structured agent, project=%s, job=%s",
            build_id or "?",
            project_name,
            job_name,
        )
        if build_id:
            buildlog.log_step(
                build_id, project_name, job_name, "agent_start", "Structured agent started"
            )

        max_draft_iterations = 3
        draft_iteration = 0
        extra_context: str | None = None

        while time.monotonic() < deadline:
            draft_iteration += 1
            if draft_iteration > max_draft_iterations:
                log.warning("Build %s: max draft iterations reached", build_id or "?")
                return {
                    "status": "failed",
                    "summary": "Max validation iterations reached",
                    "build_id": build_id,
                }

            try:
                parsed = self._parse(
                    user_message,
                    model=model,
                    system_prompt=system_prompt,
                    extra_context=extra_context,
                )
            except ValueError as exc:
                log.exception("Build %s: parse failed", build_id or "?")
                return {
                    "status": "failed",
                    "summary": str(exc),
                    "build_id": build_id,
                }

            items = parsed.items or []
            if not items:
                return {
                    "status": "failed",
                    "summary": "Agent returned no items",
                    "build_id": build_id,
                }

            if build_id:
                buildlog.log_step(
                    build_id,
                    project_name,
                    job_name,
                    "llm_response",
                    json.dumps([i.model_dump() for i in items])[:4000],
                )

            # Check for pending - send back to queue and exit
            pending_items = [i for i in items if i.status == JobStatus.PENDING]
            if pending_items:
                response_text = "; ".join(
                    i.response or "Need more information" for i in pending_items
                )
                log.info("Build %s: pending status, routing to queue", build_id or "?")
                if build_id:
                    buildlog.log_step(
                        build_id, project_name, job_name, "pending", response_text
                    )
                return {
                    "status": "pending",
                    "summary": response_text,
                    "response": response_text,
                    "build_id": build_id,
                    "pending_response": response_text,
                }

            # Check for done - final response (use "complete" for queue compatibility)
            done_items = [i for i in items if i.status == JobStatus.DONE]
            if done_items and not any(i.status == JobStatus.DRAFTED for i in items):
                summary = "; ".join(i.response or i.result or "Done" for i in done_items)
                return {
                    "status": "complete",
                    "summary": summary,
                    "items": [i.model_dump() for i in done_items],
                    "build_id": build_id,
                }

            # Handle drafted items: write, validate, possibly re-prompt
            drafted = [i for i in items if i.status == JobStatus.DRAFTED]
            if not drafted:
                # Mixed or unexpected - treat as done
                summary = "; ".join(
                    i.response or i.result or str(i.status) for i in items
                )
                return {
                    "status": "complete",
                    "summary": summary,
                    "items": [i.model_dump() for i in items],
                    "build_id": build_id,
                }

            validation_outputs: list[str] = []
            for item in drafted:
                if not item.filename or not item.filecontent:
                    validation_outputs.append(
                        f"{item.filename or 'unknown'}: missing filename or filecontent"
                    )
                    continue

                # Write file
                write_result = write_pipeline(
                    project_name=project_name,
                    job_name=item.filename,
                    code=item.filecontent,
                )
                write_data = json.loads(write_result)
                if not write_data.get("success"):
                    validation_outputs.append(
                        f"{item.filename}: write failed - {write_data.get('error', write_result)}"
                    )
                    continue

                if build_id:
                    buildlog.log_step(
                        build_id,
                        project_name,
                        job_name,
                        "write_pipeline",
                        write_result[:2000],
                    )

                # Run validation (val=True for quick test)
                safe_job = item.filename.replace("-", "_")
                launch_result = launch_run(
                    project_name,
                    safe_job,
                    run_config={"ops": {"run_etl": {"config": {"val": True}}}},
                )
                launch_data = json.loads(launch_result)
                if not launch_data.get("success"):
                    validation_outputs.append(
                        f"{item.filename}: launch failed - {launch_data.get('error', launch_result)}"
                    )
                    continue

                run_id = launch_data.get("run_id")
                if build_id:
                    buildlog.log_step(
                        build_id,
                        project_name,
                        job_name,
                        "launch_run",
                        json.dumps({"run_id": run_id, "validation": True}),
                    )

                # Poll until validation run completes
                poll_start = time.monotonic()
                poll_timeout = 120
                validation_succeeded = False
                while time.monotonic() - poll_start < poll_timeout:
                    status_result = get_run_status(run_id)
                    status_data = json.loads(status_result)
                    run_status = status_data.get("status", "").upper()
                    if run_status in ("SUCCESS", "FAILURE", "CANCELED"):
                        validation_succeeded = run_status == "SUCCESS"
                        validation_outputs.append(
                            f"{item.filename}: run_id={run_id} status={run_status} "
                            f"stats={status_data.get('stats', {})}"
                        )
                        break
                    await asyncio.sleep(2)
                else:
                    validation_outputs.append(
                        f"{item.filename}: run_id={run_id} timed out waiting for completion"
                    )

                # Materialize: full run after validation passes
                if validation_succeeded:
                    mat_result = launch_run(
                        project_name,
                        safe_job,
                        run_config={"ops": {"run_etl": {"config": {"val": False}}}},
                    )
                    mat_data = json.loads(mat_result)
                    if mat_data.get("success"):
                        mat_run_id = mat_data.get("run_id")
                        if build_id:
                            buildlog.log_step(
                                build_id,
                                project_name,
                                job_name,
                                "launch_run",
                                json.dumps({"run_id": mat_run_id, "materialize": True}),
                            )
                        mat_poll_start = time.monotonic()
                        mat_poll_timeout = cfg.get("materialize_timeout_seconds", 600)
                        while time.monotonic() - mat_poll_start < mat_poll_timeout:
                            mat_status = get_run_status(mat_run_id)
                            mat_status_data = json.loads(mat_status)
                            mat_run_status = mat_status_data.get("status", "").upper()
                            if mat_run_status in ("SUCCESS", "FAILURE", "CANCELED"):
                                validation_outputs.append(
                                    f"{item.filename}: materialize run_id={mat_run_id} "
                                    f"status={mat_run_status}"
                                )
                                break
                            await asyncio.sleep(2)
                        else:
                            validation_outputs.append(
                                f"{item.filename}: materialize run_id={mat_run_id} timed out"
                            )
                    else:
                        validation_outputs.append(
                            f"{item.filename}: materialize launch failed - "
                            f"{mat_data.get('error', mat_result)}"
                        )

            # Feed validation output back to LLM for edit decision
            extra_context = (
                "Validation run results:\n"
                + "\n".join(validation_outputs)
                + "\n\nIf validation failed or produced errors, fix the code and return "
                "status=drafted with updated filecontent. If validation succeeded, "
                "return status=done."
            )
