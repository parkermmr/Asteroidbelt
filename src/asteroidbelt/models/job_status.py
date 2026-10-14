"""
JobStatus is the lifecycle of a submitted job.
"""

from enum import Enum


class JobStatus(str, Enum):
    """
    QUEUED and RUNNING are live states, everything else is terminal.
    """

    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    EXPIRED = "expired"
    EVICTED = "evicted"

    def terminal(self) -> bool:
        """
        True once the job can no longer change state.
        """
        return self not in (JobStatus.QUEUED, JobStatus.RUNNING)
