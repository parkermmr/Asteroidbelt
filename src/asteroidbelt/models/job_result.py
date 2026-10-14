"""
JobResult is the terminal outcome of a job.
"""

from dataclasses import dataclass
from typing import Any

from asteroidbelt.models.job_status import JobStatus


@dataclass
class JobResult:
    """
    The final status of a job plus its return value or error text.
    """

    name: str
    status: JobStatus
    value: Any = None
    error: str | None = None

    def successful(self) -> bool:
        return self.status == JobStatus.COMPLETED
