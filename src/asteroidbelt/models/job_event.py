"""
JobEvent is one transition in a job's lifecycle.
"""

from dataclasses import dataclass, field
from time import time

from asteroidbelt.models.job_status import JobStatus


@dataclass
class JobEvent:
    """
    Emitted to observers whenever a job changes state, giving
    external systems everything needed to persist job information,
    drive progress bars, or feed metrics exporters.
    """

    name: str
    status: JobStatus
    at: float = field(default_factory=time)
    error: str | None = None

    def describe(self) -> dict:
        """
        Serializable form for journals and wire protocols.
        """
        return {
            "name": self.name,
            "status": self.status.value,
            "at": self.at,
            "error": self.error,
        }
