"""
FileJournal persists jobs and results as files in one directory.
"""

import json
from asyncio import CancelledError, create_task, sleep, to_thread
from os import O_CREAT, O_EXCL, O_WRONLY, close, fsync, getpid
from os import open as osopen
from pathlib import Path
from secrets import token_hex
from socket import gethostname
from time import time
from urllib.parse import quote, unquote

from asteroidbelt.journal.journal import Journal

LEASE = 30.0


class FileJournal(Journal):
    """
    Journal for local or shared filesystem durability. Each job gets
    a .job file at submission and a .result file at settlement;
    writes land in a temporary file, are fsynced, then renamed, so a
    power cut never leaves a torn entry.

    Several compute instances may share one directory. A job is
    owned by whichever instance holds its .lease file, a small marker
    naming the owner and an expiry a few seconds out. A running owner
    renews its leases in the background; if it dies its leases go
    stale, and on the next replay a surviving instance steals the
    expired lease and takes the job over. Replay therefore hands each
    unfinished job to exactly one live instance, never resubmitting
    it across the pool, while a crash still fails over. The tradeoff
    is the usual at least once contract: an owner
    paused past its lease can be stolen from and the job run twice.
    """

    def __init__(
        self, path: str, instance: str | None = None, lease: float = LEASE
    ) -> None:
        self.path = Path(path)
        self.instance = instance if instance is not None else self.identity()
        self.lease = lease
        self.owned: set[str] = set()
        self.tender = None

    def identity(self) -> str:
        """
        A token unique to this process for stamping owned leases.
        """
        return f"{gethostname()}-{getpid()}-{token_hex(6)}"

    async def open(self) -> None:
        self.path.mkdir(parents=True, exist_ok=True)
        self.tender = create_task(self.tend())

    async def close(self) -> None:
        if self.tender is not None:
            self.tender.cancel()
            try:
                await self.tender
            except CancelledError:
                pass
            self.tender = None
        await to_thread(self.release)

    def release(self) -> None:
        """
        Drop the lease on every job this instance owns so a clean
        restart or a peer reclaims them at once. A crash skips this,
        leaving leases to expire and fail over on their own.
        """
        for name in list(self.owned):
            self.locate(name, "lease").unlink(missing_ok=True)
        self.owned.clear()

    def locate(self, name: str, kind: str) -> Path:
        """
        The file holding one kind of entry for one job.
        """
        return self.path / f"{quote(name, safe='')}.{kind}"

    def write(self, target: Path, blob: bytes) -> None:
        """
        Atomic durable write: temporary file, fsync, rename.
        """
        staging = target.with_name(target.name + ".tmp")
        with open(staging, "wb") as handle:
            handle.write(blob)
            handle.flush()
            fsync(handle.fileno())
        staging.replace(target)

    def stamp(self, name: str) -> None:
        """
        Overwrite this job's lease with a fresh expiry under this
        instance, marking it owned here.
        """
        marker = {"owner": self.instance, "expiry": time() + self.lease}
        self.write(self.locate(name, "lease"), json.dumps(marker).encode())
        self.owned.add(name)

    def read_lease(self, name: str) -> dict | None:
        """
        The lease marker for a job, or None when unheld or unreadable.
        """
        target = self.locate(name, "lease")
        try:
            return json.loads(target.read_bytes())
        except (FileNotFoundError, ValueError):
            return None

    def claim(self, name: str) -> bool:
        """
        Take ownership of a job for this instance. Wins when the job
        is unheld, already ours, or held by a lease that has expired;
        loses when another live instance holds a fresh lease. The
        empty create is atomic, so two instances racing an unheld job
        never both win.
        """
        marker = self.read_lease(name)
        if marker is not None:
            mine = marker.get("owner") == self.instance
            if not mine and marker.get("expiry", 0) > time():
                return False
            self.stamp(name)
            return True
        target = self.locate(name, "lease")
        try:
            handle = osopen(target, O_CREAT | O_EXCL | O_WRONLY)
        except FileExistsError:
            return False
        close(handle)
        self.stamp(name)
        return True

    def erase(self, name: str) -> None:
        """
        Drop every file for a job we own, lease included.
        """
        self.locate(name, "job").unlink(missing_ok=True)
        self.locate(name, "result").unlink(missing_ok=True)
        self.locate(name, "lease").unlink(missing_ok=True)
        self.owned.discard(name)

    def scan(self) -> tuple[dict[str, bytes], dict[str, bytes]]:
        """
        Read back every entry this instance can claim. Jobs and
        standalone results held by a live peer are left to it, so a
        shared directory hands each name to one instance only.
        """
        jobs: dict[str, bytes] = {}
        for entry in self.path.glob("*.job"):
            name = unquote(entry.name.removesuffix(".job"))
            if self.claim(name):
                jobs[name] = entry.read_bytes()
        results: dict[str, bytes] = {}
        for entry in self.path.glob("*.result"):
            name = unquote(entry.name.removesuffix(".result"))
            if name in jobs or self.claim(name):
                results[name] = entry.read_bytes()
        pending = {name: blob for name, blob in jobs.items() if name not in results}
        return pending, results

    def renew(self) -> None:
        """
        Push the expiry of every still present owned lease forward.
        """
        for name in list(self.owned):
            if self.locate(name, "lease").exists():
                self.stamp(name)
            else:
                self.owned.discard(name)

    async def tend(self) -> None:
        """
        Keep this instance's leases fresh so a live owner is never
        mistaken for a dead one. Renewal runs a third of the way into
        the lease window, well inside it to survive a missed tick.
        """
        while True:
            await sleep(max(self.lease / 3.0, 0.05))
            await to_thread(self.renew)

    async def record(self, name: str, blob: bytes) -> None:
        await to_thread(self.stamp, name)
        await to_thread(self.write, self.locate(name, "job"), blob)

    async def settle(self, name: str, blob: bytes) -> None:
        await to_thread(self.write, self.locate(name, "result"), blob)

    async def forget(self, name: str) -> None:
        await to_thread(self.erase, name)

    async def replay(self) -> tuple[dict[str, bytes], dict[str, bytes]]:
        return await to_thread(self.scan)
