from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints

ShortText = Annotated[str, StringConstraints(strip_whitespace=True, max_length=500)]


class ScrapeInput(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    query: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=500)]
    location: ShortText = ""
    country: Annotated[str, StringConstraints(to_lower=True, pattern=r"^[a-zA-Z]{2}$")] = "us"
    date_posted: Literal["any", "past_24_hours", "past_3_days", "past_week", "past_month"] = "any"
    job_type: Literal["any", "full_time", "part_time", "contract", "internship"] = "any"
    remote_only: bool = False
    max_total_results: int = Field(default=100, ge=1, le=1000)
    max_pages: int = Field(default=100, ge=1, le=100)
    timeout_seconds: int = Field(default=300, ge=30, le=300)
