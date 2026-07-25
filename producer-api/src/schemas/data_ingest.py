"""
Pydantic schema for the data ingestion payload.

The schema is intentionally flexible: it requires a small set of core fields
(user_id, event_type) while allowing an arbitrary JSON object under `details`
so that different event producers can attach whatever metadata they need.
"""
from typing import Any, Dict, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


class DataIngestPayload(BaseModel):
    user_id: str = Field(..., min_length=1, description="Identifier of the user/source generating the event")
    event_type: str = Field(..., min_length=1, description="Type/category of the event, e.g. 'page_view'")
    details: Dict[str, Any] = Field(default_factory=dict, description="Arbitrary event metadata")

    @field_validator("user_id", "event_type")
    @classmethod
    def not_blank(cls, v: str) -> str:
        if v is not None and not v.strip():
            raise ValueError("must not be blank")
        return v

    model_config = ConfigDict(
        extra="allow",  # allow additional, unspecified fields to pass through
        json_schema_extra={
            "example": {
                "user_id": "abc123",
                "event_type": "page_view",
                "details": {"path": "/home"},
            }
        },
    )


class IngestResponse(BaseModel):
    status: str
    message_id: str


class ErrorResponse(BaseModel):
    detail: str
