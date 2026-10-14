"""
JobRecord is the bookkeeping a compute backend keeps for one job.
"""

from dataclasses import dataclass, field
from time import time

from asteroidbelt.models.job_status import JobStatus


@dataclass
class JobRecord:
    """
    Tracks one job from submission to its terminal state.
    """

    name: str
    cpus: float
    ttl: float
    status: JobStatus = JobStatus.QUEUED
    submitted: float = field(default_factory=time)
    started: float | None = None
    finished: float | None = None

    def expired(self) -> bool:
        """
        True when the job has outlived its wall time budget.
        """
        opened = self.started if self.started is not None else self.submitted
        return not self.status.terminal() and time() - opened > self.ttl

    def begin(self) -> None:
        """
        Mark the job running from now.
        """
        self.status = JobStatus.RUNNING
        self.started = time()

    def finish(self, status: JobStatus) -> None:
        self.status = status
        self.finished = time()

    def describe(self) -> dict:
        """
        Serializable summary for APIs and logs.
        """
        end = self.finished or time()
        return {
            "name": self.name,
            "status": self.status.value,
            "cpus": self.cpus,
            "ttl": self.ttl,
            "submitted": self.submitted,
            "started": self.started,
            "finished": self.finished,
            "queued_seconds": round((self.started or end) - self.submitted, 2),
            "running_seconds": (
                round(end - self.started, 2) if self.started is not None else None
            ),
        }
