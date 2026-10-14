"""
Compute is the interface for running named jobs somewhere else.

It adds the policy schedulers rarely give you natively: unique
names so every job is addressable, wall time budgets enforced by a
watchdog, eviction of queued or running jobs, and a bounded history
of terminal outcomes. Submission is idempotent by name, so redelivery
from an upstream broker is safe.

Finished jobs also land on a consumption queue with Kafka style
offsets: next and poll read results, commit acknowledges them, and
rollback rereads everything unacknowledged after a downstream
failure. Observers receive a JobEvent on every transition, which is
the hook for progress bars and metrics exporters; by default the
library stores nothing and depends on nothing.

Attach a Journal for durability: every submission is written ahead
of its launch and every terminal result written after settlement.
After a crash or power loss, start replays the journal, making
settled results pollable again and resubmitting jobs that never
reached one. Execution and delivery are therefore at least once;
commit acknowledges results into the journal so a restart does not
redeliver them.
"""

import abc
import asyncio
import logging
from collections import deque
from concurrent.futures import Future
from threading import Event, RLock, Thread
from time import monotonic
from typing import Any, Callable

from asteroidbelt.codec.pickle_serializer import PickleSerializer
from asteroidbelt.codec.serializer import Serializer
from asteroidbelt.journal.journal import Journal
from asteroidbelt.models.bundle import Bundle
from asteroidbelt.models.job import Job
from asteroidbelt.models.job_event import JobEvent
from asteroidbelt.models.job_record import JobRecord
from asteroidbelt.models.job_result import JobResult
from asteroidbelt.models.job_status import JobStatus

logger = logging.getLogger(__name__)

HISTORY = 500

Observer = Callable[[JobEvent], None]


class Compute(abc.ABC):
    """
    Async facade over a compute backend.

    Callers submit a named job, then poll for its JobResult by name
    or consume terminal results in finish order through next, poll,
    and commit. A poll of None means the job is still queued or
    running.

    Reaping runs on a private daemon thread with its own event loop,
    not on the caller's loop, so finished jobs settle and jobs that
    outlive their ttl are evicted even while the driver blocks in a
    tight consume loop or synchronous work. A lock serializes every
    mutation of the shared bookkeeping, so the reaper and the driver
    never see each other's half written state.
    """

    def __init__(
        self,
        sweep: float = 2.0,
        journal: Journal | None = None,
        serializer: Serializer | None = None,
    ) -> None:
        self.sweep = sweep
        self.journal = journal
        self.serializer = serializer if serializer is not None else PickleSerializer()
        self.writes: set[Future] = set()
        self.closing = False
        self.records: dict[str, JobRecord] = {}
        self.finished: dict[str, JobRecord] = {}
        self.results: dict[str, JobResult] = {}
        self.history: deque[str] = deque(maxlen=HISTORY)
        self.queue: deque[str] = deque()
        self.cursor = 0
        self.observers: list[Observer] = []
        self.counters: dict[str, float] = {
            "submitted": 0,
            "completed": 0,
            "failed": 0,
            "evicted": 0,
            "expired": 0,
            "seconds_queued": 0.0,
            "seconds_running": 0.0,
        }
        self.lock = RLock()
        self.reaper: Thread | None = None
        self.sweeper: asyncio.Task | None = None
        self.loop: asyncio.AbstractEventLoop | None = None

    async def start(self) -> None:
        """
        Launch the reaper thread, then open and replay the journal on
        its loop so every journal touch shares one loop.
        """
        self.closing = False
        self.loop = asyncio.new_event_loop()
        ready = Event()
        self.reaper = Thread(target=self.watch, args=(ready,), daemon=True)
        self.reaper.start()
        ready.wait()
        if self.journal is not None:
            await self.on_reaper(self.open_journal())

    async def open_journal(self) -> None:
        """
        Open and replay the journal. Runs on the reaper loop.
        """
        await self.journal.open()
        await self.recover()

    def on_reaper(self, coroutine) -> Any:
        """
        Run a coroutine on the reaper loop and await its result from
        the caller's loop, so journal work stays on one loop.
        """
        future = asyncio.run_coroutine_threadsafe(coroutine, self.loop)
        return asyncio.wrap_future(future)

    async def stop(self) -> None:
        """
        Drain in flight journal writes, close the journal, then wind
        down the reaper thread and its loop, all on the reaper loop so
        one loop owns the journal from open to close. Jobs still live
        at stop are not settled in the journal, so the next start
        resubmits them.
        """
        self.closing = True
        if self.loop is not None and self.reaper is not None:
            await self.on_reaper(self.drain())
            self.loop.call_soon_threadsafe(self.loop.stop)
            await asyncio.to_thread(self.reaper.join, 30.0)
            self.loop.close()
        self.reaper = None
        self.loop = None

    def watch(self, ready: Event) -> None:
        """
        Reaper thread body. Runs a private event loop that sweeps on
        an interval, reaping finished jobs even when nobody polls them
        and evicting jobs past their ttl, and that also serves journal
        work so one loop owns the journal end to end.
        """
        asyncio.set_event_loop(self.loop)
        self.sweeper = self.loop.create_task(self.reap())
        ready.set()
        self.loop.run_forever()

    async def reap(self) -> None:
        """
        Reap or expire every live job once per interval.
        """
        while not self.closing:
            await asyncio.sleep(self.sweep)
            for record in list(self.records.values()):
                if record.expired():
                    await self.expire(record.name)
                else:
                    await self.collect(record.name)

    async def drain(self) -> None:
        """
        Stop the sweeper, wait for outstanding journal writes to land,
        then close the journal, so a clean stop loses no durability.
        """
        if self.sweeper is not None:
            self.sweeper.cancel()
            try:
                await self.sweeper
            except asyncio.CancelledError:
                pass
            self.sweeper = None
        for future in list(self.writes):
            try:
                await asyncio.wrap_future(future)
            except Exception:
                pass
        if self.journal is not None:
            await self.journal.close()

    def subscribe(self, observer: Observer) -> None:
        """
        Register a callback receiving a JobEvent on every job
        transition. Observers must be fast and never raise; failures
        are logged and swallowed.
        """
        self.observers.append(observer)

    def notify(self, name: str, status: JobStatus, error: str | None = None) -> None:
        """
        Emit a transition event to every observer.
        """
        event = JobEvent(name=name, status=status, error=error)
        for observer in self.observers:
            try:
                observer(event)
            except Exception:
                logger.exception("Observer failed on job %s", name)

    async def submit(
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
        """
        Submit a named job. Returns False when the name is already
        queued or running, making submission idempotent by name.
        With a journal, the job is durably recorded before launch;
        the function, its arguments, and any bound instance must be
        serializable by the compute's serializer, or submit raises.
        Name files or directories the function reads and they are
        packed and shipped with it, arriving in a temporary directory
        passed as the function's first argument; hand in a prepacked
        Bundle instead to reuse one archive across submissions.
        """
        if files is not None and bundle is not None:
            raise ValueError("pass files or bundle, not both")
        if files is not None:
            bundle = Bundle(files=files)
        if bundle is not None:
            function = bundle.wrap(function)
        job = Job(
            name=name,
            function=function,
            arguments=arguments,
            keywords=keywords if keywords is not None else {},
            cpus=cpus,
            ttl=ttl,
        )
        blob = self.serializer.dumps(job) if self.journal is not None else None
        with self.lock:
            if name in self.records:
                return False
            self.records[name] = JobRecord(name=name, cpus=cpus, ttl=ttl)
            self.counters["submitted"] += 1
        if blob is not None:
            await self.on_reaper(self.journal.record(name, blob))
        self.notify(name, JobStatus.QUEUED)
        await self.launch(job)
        return True

    def started(self, name: str) -> None:
        """
        Mark a queued job running. Backends call this the moment the
        scheduler picks the job up.
        """
        record = self.records.get(name)
        if record is None or record.status != JobStatus.QUEUED:
            return
        record.begin()
        self.notify(name, JobStatus.RUNNING)

    async def poll(self, target: str | int) -> Any:
        """
        With a name, return that job's terminal JobResult, or None
        while it is still queued or running; unknown names raise
        KeyError. With a count, consume up to that many unread
        results from the completed queue in finish order.
        """
        if isinstance(target, str):
            with self.lock:
                if target in self.results:
                    return self.results[target]
                if target not in self.records:
                    raise KeyError(f"No job named {target!r}")
            await self.collect(target)
            return self.results.get(target)
        await self.refresh()
        with self.lock:
            consumed: list[JobResult] = []
            while len(consumed) < target and self.cursor < len(self.queue):
                consumed.append(self.read())
            return consumed

    async def wait(
        self, name: str, timeout: float | None = None, interval: float = 0.1
    ) -> JobResult | None:
        """
        Block until the named job settles and return its JobResult,
        or None when the timeout elapses first.
        """
        deadline = monotonic() + timeout if timeout is not None else None
        while True:
            result = await self.poll(name)
            if result is not None:
                return result
            if deadline is not None and monotonic() >= deadline:
                return None
            await asyncio.sleep(interval)

    async def join(self, timeout: float | None = None, interval: float = 0.1) -> bool:
        """
        Block until every live job has settled, so a fire and forget
        caller can stop without evicting work still running. Returns
        False when the timeout elapses with jobs still live.
        """
        deadline = monotonic() + timeout if timeout is not None else None
        while True:
            await self.refresh()
            if not self.records:
                return True
            if deadline is not None and monotonic() >= deadline:
                return False
            await asyncio.sleep(interval)

    async def next(self) -> JobResult | None:
        """
        The next unread result in the completed queue, or None when
        nothing new has finished.
        """
        await self.refresh()
        with self.lock:
            if self.cursor >= len(self.queue):
                return None
            return self.read()

    def read(self) -> JobResult:
        """
        Return the result at the cursor and advance it.
        """
        name = self.queue[self.cursor]
        self.cursor += 1
        return self.results[name]

    def commit(self) -> int:
        """
        Acknowledge every read result, removing it from the queue for
        good, and return how many were committed. Uncommitted reads
        stay claimable through rollback. With a journal, committed
        results are forgotten there too, so a restart never
        redelivers them.
        """
        with self.lock:
            committed = self.cursor
            names = [self.queue.popleft() for _ in range(self.cursor)]
            self.cursor = 0
            if self.journal is not None:
                for name in names:
                    self.write(self.journal.forget(name), name)
            return committed

    def rollback(self) -> int:
        """
        Rewind the cursor so every uncommitted result is read again,
        the recovery path when a downstream consumer fails. Returns
        how many results became readable again.
        """
        with self.lock:
            rewound = self.cursor
            self.cursor = 0
            return rewound

    def find(self, name: str) -> JobRecord | None:
        """
        The record for a job, live or finished, or None.
        """
        return self.records.get(name) or self.finished.get(name)

    def offset(self) -> int:
        """
        How many finished results wait unread in the queue.
        """
        return len(self.queue) - self.cursor

    def pending(self) -> int:
        """
        How many submitted jobs the scheduler has not started yet.
        """
        return sum(
            1 for record in self.records.values() if record.status == JobStatus.QUEUED
        )

    async def refresh(self) -> None:
        """
        Sweep the backend once so every finished job is settled.
        """
        for record in list(self.records.values()):
            await self.collect(record.name)

    def jobs(self) -> list[dict]:
        """
        Describe every live job, then recent finished jobs newest
        first, including the error text of failed jobs.
        """
        live = [{**record.describe(), "error": None} for record in self.records.values()]
        done = []
        for name in reversed(self.history):
            if name not in self.finished:
                continue
            entry = self.finished[name].describe()
            result = self.results.get(name)
            entry["error"] = result.error if result is not None else None
            done.append(entry)
        return live + done

    def progress(self) -> dict:
        """
        A progress bar shaped summary of everything submitted so far.
        """
        done = (
            self.counters["completed"]
            + self.counters["failed"]
            + self.counters["evicted"]
            + self.counters["expired"]
        )
        total = self.counters["submitted"]
        return {
            "total": int(total),
            "done": int(done),
            "completed": int(self.counters["completed"]),
            "failed": int(done - self.counters["completed"]),
            "running": len(self.records) - self.pending(),
            "queued": self.pending(),
            "ratio": round(done / total, 4) if total else 1.0,
        }

    def metrics(self) -> dict:
        """
        Flat counters and gauges shaped for scraping into systems
        like Prometheus; exporting them is the integrator's job.
        """
        return {
            "jobs_submitted_total": int(self.counters["submitted"]),
            "jobs_completed_total": int(self.counters["completed"]),
            "jobs_failed_total": int(self.counters["failed"]),
            "jobs_evicted_total": int(self.counters["evicted"]),
            "jobs_expired_total": int(self.counters["expired"]),
            "jobs_running": len(self.records) - self.pending(),
            "jobs_queued": self.pending(),
            "queue_unread": self.offset(),
            "seconds_queued_total": round(self.counters["seconds_queued"], 3),
            "seconds_running_total": round(self.counters["seconds_running"], 3),
        }

    def snapshot(self) -> dict:
        """
        A serializable picture of the compute for external
        persistence: every job description, the unread queue, and
        the lifetime counters. Values are not included; results are
        arbitrary objects and saving them is the caller's call.
        """
        return {
            "jobs": self.jobs(),
            "unread": list(self.queue)[self.cursor :],
            "uncommitted": list(self.queue)[: self.cursor],
            "metrics": self.metrics(),
        }

    def settle(self, name: str, result: JobResult) -> None:
        """
        Move a job from live records into results, history, and the
        consumption queue, then tell the observers. Idempotent: a job
        already settled by a competing poll and sweep is left alone.
        """
        with self.lock:
            if name in self.results:
                return
            record = self.records.pop(name, None)
            if record is not None:
                record.finish(result.status)
                self.finished[name] = record
                entry = record.describe()
                self.counters["seconds_queued"] += entry["queued_seconds"] or 0.0
                self.counters["seconds_running"] += entry["running_seconds"] or 0.0
            if result.status.value in self.counters:
                self.counters[result.status.value] += 1
            self.results[name] = result
            if len(self.history) == self.history.maxlen:
                oldest = self.history[0]
                self.finished.pop(oldest, None)
                self.results.pop(oldest, None)
                self.forget(oldest)
                if self.journal is not None:
                    self.write(self.journal.forget(oldest), oldest)
            self.history.append(name)
            self.queue.append(name)
            self.keep(name, result)
            self.notify(name, result.status, error=result.error)

    async def recover(self) -> None:
        """
        Replay the journal: unacknowledged results become pollable
        and redeliverable again, and jobs that never reached one are
        resubmitted. Across restarts this makes execution and
        delivery at least once.
        """
        pending, settled = await self.journal.replay()
        for name, blob in settled.items():
            try:
                self.results[name] = self.serializer.loads(blob)
            except Exception:
                logger.exception("Could not recover result for job %s", name)
                continue
            self.history.append(name)
            self.queue.append(name)
        for name, blob in pending.items():
            try:
                job = self.serializer.loads(blob)
            except Exception:
                logger.exception("Could not recover job %s", name)
                continue
            await self.submit(
                job.function,
                job.name,
                arguments=job.arguments,
                keywords=job.keywords,
                cpus=job.cpus,
                ttl=job.ttl,
            )
        if pending or settled:
            logger.info(
                "Recovered %d results and resubmitted %d jobs from the journal",
                len(settled),
                len(pending),
            )

    def keep(self, name: str, result: JobResult) -> None:
        """
        Persist a terminal result to the journal in the background.
        A result whose value the serializer rejects is stored
        without the value; a job settled during shutdown is not
        stored at all, so the next start resubmits it.
        """
        if self.journal is None or self.closing:
            return
        try:
            blob = self.serializer.dumps(result)
        except Exception:
            stub = JobResult(
                name=name,
                status=result.status,
                error=result.error or "unserializable result value",
            )
            blob = self.serializer.dumps(stub)
        self.write(self.journal.settle(name, blob), name)

    def write(self, coroutine, name: str) -> None:
        """
        Run a journal write on the reaper loop, holding a reference
        until it lands, so every write shares one loop no matter which
        thread settled the job. A failed write only costs durability,
        so it is logged and swallowed.
        """
        if self.closing or self.loop is None:
            coroutine.close()
            return
        future = asyncio.run_coroutine_threadsafe(
            self.guard(coroutine, name), self.loop
        )
        self.writes.add(future)
        future.add_done_callback(self.writes.discard)

    async def guard(self, coroutine, name: str) -> None:
        """
        Await a journal write, logging any failure.
        """
        try:
            await coroutine
        except Exception:
            logger.exception("Journal write failed for job %s", name)

    def forget(self, name: str) -> None:
        """
        Drop a history evicted job from the consumption queue so a
        reader never dereferences a purged result.
        """
        try:
            index = self.queue.index(name)
        except ValueError:
            return
        del self.queue[index]
        if index < self.cursor:
            self.cursor -= 1

    @abc.abstractmethod
    async def launch(self, job: Job) -> None:
        """
        Hand a job to the backend scheduler.
        """

    @abc.abstractmethod
    async def collect(self, name: str) -> None:
        """
        Check the backend for a finished job and settle it.
        """

    @abc.abstractmethod
    async def evict(self, name: str, reason: str = "evicted") -> bool:
        """
        Force stop a queued or running job. Returns True when a live
        job was evicted.
        """

    @abc.abstractmethod
    async def expire(self, name: str) -> None:
        """
        Terminate a job that outlived its ttl.
        """

    @abc.abstractmethod
    def resources(self) -> dict:
        """
        Report total and available capacity.
        """
