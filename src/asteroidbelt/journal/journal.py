"""
Journal is the durability contract behind a Compute.

A journal remembers two facts per job: that it was submitted, and
what it finally produced. The submission is written ahead of the
launch, so a process that dies mid flight leaves a job entry with
no result; replay surfaces exactly those for resubmission, plus
every settled result that was never acknowledged. forget drops a
job once its result is committed downstream or evicted from
history.
"""

import abc


class Journal(abc.ABC):
    """
    Write ahead log for job submissions and terminal results. Every
    payload is opaque bytes produced by the compute's Serializer.
    """

    async def open(self) -> None:
        """
        Prepare backing resources; called by Compute.start.
        """

    async def close(self) -> None:
        """
        Release backing resources; called by Compute.stop.
        """

    @abc.abstractmethod
    async def record(self, name: str, blob: bytes) -> None:
        """
        Durably note that a job was submitted.
        """

    @abc.abstractmethod
    async def settle(self, name: str, blob: bytes) -> None:
        """
        Durably note a job's terminal result.
        """

    @abc.abstractmethod
    async def forget(self, name: str) -> None:
        """
        Drop everything stored for a job.
        """

    @abc.abstractmethod
    async def replay(self) -> tuple[dict[str, bytes], dict[str, bytes]]:
        """
        Read the whole journal back as (pending, settled): jobs
        submitted but never settled, and unacknowledged results.
        """
