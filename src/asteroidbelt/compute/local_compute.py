"""
LocalCompute runs jobs as local processes with a cpu budget.
"""

import logging
from collections import deque
from multiprocessing import get_context
from os import cpu_count, register_at_fork
from threading import RLock

from asteroidbelt.codec.serializer import Serializer
from asteroidbelt.compute.compute import Compute
from asteroidbelt.journal.journal import Journal
from asteroidbelt.models.job import Job
from asteroidbelt.models.job_result import JobResult
from asteroidbelt.models.job_status import JobStatus

logger = logging.getLogger(__name__)


def execute(function, arguments, keywords, connection):
    """
    Child process entry point. Reports the outcome over the pipe.
    """
    try:
        value = function(*arguments, **keywords)
        outcome = ("completed", value)
    except Exception as error:
        outcome = ("failed", repr(error))
    try:
        connection.send(outcome)
    except Exception as error:
        connection.send(("failed", f"unserializable result: {error!r}"))
    finally:
        connection.close()


class LocalCompute(Compute):
    """
    Development and single machine backend.

    Each job runs in its own process so eviction and ttl enforcement
    can kill it. Jobs queue in submission order until enough of the
    cpu budget is free.
    """

    def __init__(
        self,
        cpus: float | None = None,
        sweep: float = 2.0,
        journal: Journal | None = None,
        serializer: Serializer | None = None,
    ) -> None:
        super().__init__(sweep=sweep, journal=journal, serializer=serializer)
        self.total = cpus if cpus is not None else float(cpu_count() or 1)
        self.available = self.total
        self.backlog: deque[Job] = deque()
        self.running: dict[str, tuple[Job, object, object]] = {}
        self.context = get_context("fork")
        register_at_fork(after_in_child=self.reset)

    def reset(self) -> None:
        """
        A forked child inherits the parent's lock in whatever state
        the reaper left it. The child only runs its function and
        exits, so give it a fresh lock rather than a possibly held
        one.
        """
        self.lock = RLock()

    async def launch(self, job: Job) -> None:
        with self.lock:
            if job.cpus > self.total:
                self.settle(
                    job.name,
                    JobResult(
                        name=job.name,
                        status=JobStatus.FAILED,
                        error=f"job wants {job.cpus} cpus, machine has {self.total}",
                    ),
                )
                return
            self.backlog.append(job)
            self.schedule()

    def schedule(self) -> None:
        """
        Start queued jobs in order while the cpu budget allows.
        """
        while self.backlog and self.backlog[0].cpus <= self.available:
            job = self.backlog.popleft()
            parent, child = self.context.Pipe(duplex=False)
            process = self.context.Process(
                target=execute,
                args=(job.function, job.arguments, job.keywords, child),
                daemon=True,
            )
            process.start()
            child.close()
            self.available -= job.cpus
            self.running[job.name] = (job, process, parent)
            self.started(job.name)
            logger.info("Job %s started (cpus=%s)", job.name, job.cpus)

    async def collect(self, name: str) -> None:
        with self.lock:
            entry = self.running.get(name)
            if entry is None:
                return
            job, process, connection = entry
            if connection.poll():
                kind, payload = connection.recv()
                if kind == "completed":
                    result = JobResult(
                        name=name, status=JobStatus.COMPLETED, value=payload
                    )
                else:
                    result = JobResult(
                        name=name, status=JobStatus.FAILED, error=payload
                    )
            elif not process.is_alive():
                result = JobResult(
                    name=name,
                    status=JobStatus.FAILED,
                    error=f"process died with exit code {process.exitcode}",
                )
            else:
                return
            self.release(name)
            self.settle(name, result)

    async def evict(self, name: str, reason: str = "evicted") -> bool:
        return self.terminate(name, JobStatus.EVICTED, reason)

    async def expire(self, name: str) -> None:
        self.terminate(name, JobStatus.EXPIRED, "ttl exceeded")

    def terminate(self, name: str, status: JobStatus, reason: str) -> bool:
        """
        Kill a running job or drop a queued one.
        """
        with self.lock:
            for job in list(self.backlog):
                if job.name == name:
                    self.backlog.remove(job)
                    self.settle(name, JobResult(name=name, status=status, error=reason))
                    logger.info(
                        "Job %s %s while queued: %s", name, status.value, reason
                    )
                    return True
            if name in self.running:
                self.release(name, kill=True)
                self.settle(name, JobResult(name=name, status=status, error=reason))
                logger.info("Job %s %s: %s", name, status.value, reason)
                return True
            return False

    def release(self, name: str, kill: bool = False) -> None:
        """
        Reclaim the cpu budget of a finished or killed job.
        """
        job, process, connection = self.running.pop(name)
        if kill and process.is_alive():
            process.kill()
        process.join(timeout=5)
        connection.close()
        self.available += job.cpus
        self.schedule()

    def resources(self) -> dict:
        return {
            "total_cpus": self.total,
            "available_cpus": self.available,
            "running": len(self.running),
            "queued": len(self.backlog),
        }

    async def stop(self) -> None:
        await super().stop()
        for name in list(self.running):
            self.terminate(name, JobStatus.EVICTED, "compute stopped")
        self.backlog.clear()
