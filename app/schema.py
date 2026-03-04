"""Pydantic schemas for the Dagster agent's structured output. Defines the three-state (pending/drafted/done) pipeline lifecycle."""

from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, Field


class JobStatus(str, Enum):
    """Pipeline item lifecycle state."""

    PENDING = "pending"
    DRAFTED = "drafted"
    DONE = "done"  # Maps to "complete" in final response for queue compatibility


class JobOutputItem(BaseModel):
    """Single pipeline item with status, generated code, and validation result."""

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
    """LLM structured response containing a list of pipeline items."""

    items: list[JobOutputItem] = Field(
        default_factory=list,
        description="Array of job output items (files, statuses, responses)",
    )
