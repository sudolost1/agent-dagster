from __future__ import annotations

from pydantic import BaseModel


class ParseResult(BaseModel):
    """Default structured output schema for agent.parse().

    Extend or replace this model to match your domain.
    """

    answer: str
    confidence: float
    reasoning: str
