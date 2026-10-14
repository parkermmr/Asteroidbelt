"""
SyncCompute is a blocking facade over any Compute.
"""

import asyncio
from threading import Event, Thread
from typing import Any, Callable

from asteroidbelt.compute.compute import Compute, Observer
from asteroidbelt.models.bundle import Bundle
from asteroidbelt.models.job_record import JobRecord
from asteroidbelt.models.job_result import JobResult


class SyncCompute:
    """
    Runs a Compute on a private event loop thread and exposes its
    whole surface as plain blocking calls, for scripts, notebooks,
    and services that never touch asyncio. Every call, reads
    included, executes on the loop thread, so the wrapped compute
    never sees concurrent access.

    Usage:

        with SyncCompute(LocalCompute()) as compute:
            compute.submit(work, name="job-1")
            result = compute.wait("job-1", timeout=60.0)

    Or fire and forget without a context, submitting now and reaping
    later, and closing with a drain so in flight jobs finish instead
    of being evicted:

        compute = SyncCompute(LocalCompute())
        compute.start()
        compute.submit(work, name="job-1")
        result = compute.poll("job-1")
        compute.close(drain=True)
    """

    def __init__(self, compute: Compute) -> None:
        self.compute = compute
        self.loop: asyncio.AbstractEventLoop | None = None
        self.thread: Thread | None = None

    def start(self) -> None:
        """
        Spin up the loop thread and start the wrapped compute.
        """
        ready = Event()
        self.thread = Thread(target=self.spin, args=(ready,), daemon=True)
        self.thread.start()
        ready.wait()
        self.call(self.compute.start())

    def stop(self) -> None:
        """
        Stop the wrapped compute, evicting any job still live, then
        wind down the loop thread.
        """
        self.close(drain=False)

    def close(self, drain: bool = True, timeout: float | None = None) -> None:
        """
        Wind down. With drain, block until every live job settles so
        fire and forget work is reaped rather than evicted; without
        it, live jobs are evicted at stop.
        """
        if self.loop is None:
            return
        if drain:
            self.call(self.compute.join(timeout=timeout))
        self.call(self.compute.stop())
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join()
        self.loop.close()
        self.loop = None
        self.thread = None

    def spin(self, ready: Event) -> None:
        """
        Loop thread body.
        """
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        ready.set()
        self.loop.run_forever()

    def call(self, coroutine) -> Any:
        """
        Run a coroutine on the loop thread and block for its result.
        """
        if self.loop is None:
            raise RuntimeError("SyncCompute is not started")
        return asyncio.run_coroutine_threadsafe(coroutine, self.loop).result()

    async def invoke(self, function: Callable, *arguments) -> Any:
        return function(*arguments)

    def relay(self, function: Callable, *arguments) -> Any:
        """
        Run a plain method of the wrapped compute on the loop thread.
        """
        return self.call(self.invoke(function, *arguments))

    def submit(
        self,
        function: Callable,
        name: str,
        arguments: tuple = (),
        keywords: dict[str, Any] | None = None,
        cpus: float = 1.0,
        ttl: float = 3600.0,
        files: list[str] | None = None,
        bundle: Bundle | None = None,
    ) -> bool:
        return self.call(
            self.compute.submit(
                function,
                name,
                arguments=arguments,
                keywords=keywords,
                cpus=cpus,
                ttl=ttl,
                files=files,
                bundle=bundle,
            )
        )

    def poll(self, target: str | int) -> Any:
        return self.call(self.compute.poll(target))

    def wait(
        self, name: str, timeout: float | None = None, interval: float = 0.1
    ) -> JobResult | None:
        return self.call(self.compute.wait(name, timeout=timeout, interval=interval))

    def join(self, timeout: float | None = None, interval: float = 0.1) -> bool:
        return self.call(self.compute.join(timeout=timeout, interval=interval))

    def next(self) -> JobResult | None:
        return self.call(self.compute.next())

    def evict(self, name: str, reason: str = "evicted") -> bool:
        return self.call(self.compute.evict(name, reason))

    def subscribe(self, observer: Observer) -> None:
        self.relay(self.compute.subscribe, observer)

    def commit(self) -> int:
        return self.relay(self.compute.commit)

    def rollback(self) -> int:
        return self.relay(self.compute.rollback)

    def offset(self) -> int:
        return self.relay(self.compute.offset)

    def pending(self) -> int:
        return self.relay(self.compute.pending)

    def find(self, name: str) -> JobRecord | None:
        return self.relay(self.compute.find, name)

    def jobs(self) -> list[dict]:
        return self.relay(self.compute.jobs)

    def progress(self) -> dict:
        return self.relay(self.compute.progress)

    def metrics(self) -> dict:
        return self.relay(self.compute.metrics)

    def resources(self) -> dict:
        return self.relay(self.compute.resources)

    def snapshot(self) -> dict:
        return self.relay(self.compute.snapshot)

    def __enter__(self) -> "SyncCompute":
        self.start()
        return self

    def __exit__(self, kind, error, trace) -> None:
        self.close(drain=error is None)
