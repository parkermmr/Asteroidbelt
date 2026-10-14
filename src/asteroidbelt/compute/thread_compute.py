"""
ThreadCompute runs jobs on a shared thread pool.
"""

import asyncio
import logging
from concurrent.futures import Future, ThreadPoolExecutor
from os import cpu_count

from asteroidbelt.codec.serializer import Serializer
from asteroidbelt.compute.compute import Compute
from asteroidbelt.journal.journal import Journal
from asteroidbelt.models.job import Job
from asteroidbelt.models.job_result import JobResult
from asteroidbelt.models.job_status import JobStatus

logger = logging.getLogger(__name__)


class ThreadCompute(Compute):
    """
    Lightweight backend for io bound jobs and free threaded builds,
    where threads give true parallelism without process overhead.

    The pool queues jobs beyond its worker count in submission
    order. A running thread cannot be killed, so evicting one fails
    and expiring one settles the record while the thread finishes in
    the background; its late result is discarded.
    """

    def __init__(
        self,
        workers: int | None = None,
        sweep: float = 2.0,
        journal: Journal | None = None,
        serializer: Serializer | None = None,
    ) -> None:
        super().__init__(sweep=sweep, journal=journal, serializer=serializer)
        self.workers = workers if workers is not None else (cpu_count() or 1) * 4
        self.executor: ThreadPoolExecutor | None = None
        self.loop: asyncio.AbstractEventLoop | None = None
        self.futures: dict[str, Future] = {}

    async def start(self) -> None:
        self.loop = asyncio.get_running_loop()
        self.executor = ThreadPoolExecutor(max_workers=self.workers)
        await super().start()

    async def stop(self) -> None:
        await super().stop()
        for name in list(self.futures):
            await self.evict(name, "compute stopped")
        if self.executor is not None:
            self.executor.shutdown(wait=False, cancel_futures=True)
            self.executor = None

    async def launch(self, job: Job) -> None:
        with self.lock:
            self.futures[job.name] = self.executor.submit(self.execute, job)
        logger.info("Job %s submitted to thread pool", job.name)

    def execute(self, job: Job):
        """
        Worker thread entry point. Marks the job running on the
        event loop before calling the function.
        """
        self.loop.call_soon_threadsafe(self.started, job.name)
        return job.function(*job.arguments, **job.keywords)

    async def collect(self, name: str) -> None:
        with self.lock:
            future = self.futures.get(name)
            if future is None or not future.done():
                return
            del self.futures[name]
            self.started(name)
            try:
                value = future.result()
                result = JobResult(name=name, status=JobStatus.COMPLETED, value=value)
            except Exception as error:
                result = JobResult(
                    name=name, status=JobStatus.FAILED, error=repr(error)
                )
            self.settle(name, result)

    async def evict(self, name: str, reason: str = "evicted") -> bool:
        return self.abandon(name, JobStatus.EVICTED, reason)

    async def expire(self, name: str) -> None:
        self.abandon(name, JobStatus.EXPIRED, "ttl exceeded")

    def abandon(self, name: str, status: JobStatus, reason: str) -> bool:
        """
        Cancel a queued job, or settle a running one and let its
        thread finish unobserved.
        """
        with self.lock:
            future = self.futures.pop(name, None)
            if future is None:
                return False
            future.cancel()
            self.settle(name, JobResult(name=name, status=status, error=reason))
            logger.info("Job %s %s: %s", name, status.value, reason)
            return True

    def resources(self) -> dict:
        running = len(self.records) - self.pending()
        return {
            "workers": self.workers,
            "running": running,
            "queued": self.pending(),
        }
