"""Structured output schema for the Dagster pipeline builder agent."""

from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, Field


class JobStatus(str, Enum):
    """Status of a job output item."""

    PENDING = "pending"
    DRAFTED = "drafted"
    DONE = "done"  # Maps to "complete" in final response for queue compatibility


class JobOutputItem(BaseModel):
    """Single item in the agent's structured output.

    The agent may return multiple items when it decides to write several
    files to complete a task.
    """

    status: JobStatus = Field(
        description="pending = need more info, drafted = file ready for validation, done = complete"
    )
    filename: str = Field(
        default="",
        description="Job name / module filename (without .py)",
    )
    filecontent: str = Field(
        default="",
        description="Full Python source code for the Dagster DTL job",
    )
    result: str = Field(
        default="",
        description="Execution result or validation output",
    )
    response: str = Field(
        default="",
        description="Human-readable message: pending question, detail request, or summary",
    )


class AgentOutput(BaseModel):
    """Root structured output from the agent.

    Contains an array of job output items. The agent may produce multiple
    files in a single response.
    """

    items: list[JobOutputItem] = Field(
        default_factory=list,
        description="Array of job output items (files, statuses, responses)",
    )
