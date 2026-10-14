"""
RayCompute runs jobs on a Ray cluster.
"""

import asyncio
import logging

from asteroidbelt.codec.serializer import Serializer
from asteroidbelt.compute.compute import Compute
from asteroidbelt.journal.journal import Journal
from asteroidbelt.models.job import Job
from asteroidbelt.models.job_result import JobResult
from asteroidbelt.models.job_status import JobStatus

logger = logging.getLogger(__name__)


def call(function, arguments, keywords):
    """
    Ray worker entry point.
    """
    return function(*arguments, **keywords)


class RayCompute(Compute):
    """
    Cluster backend delegating scheduling and queueing to Ray.

    Ray owns the queue, so a job is recorded as running from launch
    and its ttl counts from submission. Ray reruns a task whose
    worker died up to retries times; pair the compute with a
    journal to also survive driver or head node loss, where replay
    resubmits every unfinished job on the next start. Requires the
    ray package.

    The namespace isolates this compute's actors and named objects
    on a shared cluster. Whoever runs the cluster for several
    tenants should derive it from something the tenant cannot
    choose, never from user code.
    """

    def __init__(
        self,
        address: str | None = None,
        namespace: str | None = None,
        sweep: float = 2.0,
        retries: int = 3,
        journal: Journal | None = None,
        serializer: Serializer | None = None,
    ) -> None:
        super().__init__(sweep=sweep, journal=journal, serializer=serializer)
        self.retries = retries
        self.address = address
        self.namespace = namespace
        self.ray = None
        self.references: dict[str, object] = {}

    async def start(self) -> None:
        import ray

        self.ray = ray
        if not ray.is_initialized():
            ray.init(
                address=self.address,
                namespace=self.namespace,
                ignore_reinit_error=True,
            )
        await super().start()

    async def launch(self, job: Job) -> None:
        remote = self.ray.remote(num_cpus=job.cpus, max_retries=self.retries)(call)
        with self.lock:
            self.references[job.name] = remote.remote(
                job.function, job.arguments, job.keywords
            )
            self.started(job.name)
        logger.info("Job %s submitted to ray (cpus=%s)", job.name, job.cpus)

    async def collect(self, name: str) -> None:
        with self.lock:
            reference = self.references.get(name)
        if reference is None:
            return
        ready, waiting = await asyncio.to_thread(
            self.ray.wait, [reference], timeout=0
        )
        if not ready:
            return
        try:
            value = await asyncio.to_thread(self.ray.get, reference)
            result = JobResult(name=name, status=JobStatus.COMPLETED, value=value)
        except Exception as error:
            result = JobResult(name=name, status=JobStatus.FAILED, error=repr(error))
        with self.lock:
            if self.references.pop(name, None) is None:
                return
            self.settle(name, result)

    async def evict(self, name: str, reason: str = "evicted") -> bool:
        return self.cancel(name, JobStatus.EVICTED, reason)

    async def expire(self, name: str) -> None:
        self.cancel(name, JobStatus.EXPIRED, "ttl exceeded")

    def cancel(self, name: str, status: JobStatus, reason: str) -> bool:
        with self.lock:
            reference = self.references.pop(name, None)
            if reference is None:
                return False
            self.ray.cancel(reference, force=True)
            self.settle(name, JobResult(name=name, status=status, error=reason))
            logger.info("Job %s %s: %s", name, status.value, reason)
            return True

    def resources(self) -> dict:
        if self.ray is None:
            return {}
        return {
            "total": self.ray.cluster_resources(),
            "available": self.ray.available_resources(),
        }
