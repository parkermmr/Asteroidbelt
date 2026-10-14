"""
Examples for asteroidbelt.

Example 1: named jobs on local processes, polled by name.
Example 2: results consumed queue style, with commit and rollback.
Example 3: a journal that survives a restart.
Example 4: files shipped with a job, through the blocking facade.

Run it from anywhere; scratch files go to a temporary directory:

    python example.py
"""

import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory
from time import sleep

from asteroidbelt import FileJournal, LocalCompute, SyncCompute, ThreadCompute


def square(value: int) -> int:
    """Stand in for real work."""
    sleep(0.2)
    return value * value


def explode(value: int) -> int:
    """A job that fails."""
    raise ValueError(f"cannot handle {value}")


def count(room: str, word: str) -> int:
    """Count a word in the bundled notes file, unpacked into room."""
    return Path(room, "notes.txt").read_text().split().count(word)


async def by_name() -> None:
    """
    Submit named jobs with a cpu budget and a ttl, then poll each by
    name. Submitting a live name again is refused, so redelivery from
    a broker never runs the same work twice.
    """
    compute = LocalCompute(cpus=2, sweep=0.1)
    compute.subscribe(lambda event: print(f"  event {event.name} {event.status.value}"))
    await compute.start()
    try:
        await compute.submit(square, name="square-7", arguments=(7,), cpus=1.0, ttl=30.0)
        await compute.submit(explode, name="explode-1", arguments=(1,))
        print("  resubmitted:", await compute.submit(square, name="square-7", arguments=(7,)))
        for name in ("square-7", "explode-1"):
            result = await compute.wait(name, timeout=10)
            print(f"  {name}: {result.status.value} value={result.value} error={result.error}")
        print("  progress:", compute.progress())
    finally:
        await compute.stop()


async def as_queue() -> None:
    """
    Read finished results in finish order. Uncommitted reads come
    back after a rollback, the recovery path when a consumer fails.
    """
    compute = ThreadCompute(workers=4, sweep=0.05)
    await compute.start()
    try:
        for value in range(3):
            await compute.submit(square, name=f"job-{value}", arguments=(value,))
        await compute.join(timeout=10)
        first = await compute.poll(3)
        print("  read:", sorted(result.value for result in first))
        print("  rolled back:", compute.rollback())
        again = await compute.poll(3)
        print("  committed:", compute.commit(), "unread:", compute.offset(), len(again))
    finally:
        await compute.stop()


async def durable(directory: str) -> None:
    """
    With a journal, a settled but unacknowledged result is redelivered
    after a restart, and a job that never settled is resubmitted.
    """
    first = ThreadCompute(sweep=0.05, journal=FileJournal(directory))
    await first.start()
    await first.submit(square, name="kept", arguments=(9,))
    await first.wait("kept", timeout=10)
    await first.stop()

    second = ThreadCompute(sweep=0.05, journal=FileJournal(directory))
    await second.start()
    try:
        result = await second.next()
        print(f"  after restart: {result.name} = {result.value}")
        second.commit()
    finally:
        await second.stop()


def bundled(directory: str) -> None:
    """
    Name the files a job reads and they travel with it, unpacked into
    a directory passed as the first argument. SyncCompute needs no
    event loop of its own.
    """
    notes = Path(directory, "notes.txt")
    notes.write_text("ray local ray thread ray")
    with SyncCompute(LocalCompute(cpus=1, sweep=0.1)) as compute:
        compute.submit(count, name="count", arguments=("ray",), files=[str(notes)])
        print("  count:", compute.wait("count", timeout=10).value)


def main() -> None:
    with TemporaryDirectory() as scratch:
        print("by name")
        asyncio.run(by_name())
        print("as a queue")
        asyncio.run(as_queue())
        print("durable")
        asyncio.run(durable(str(Path(scratch, "journal"))))
        print("bundled")
        bundled(scratch)


if __name__ == "__main__":
    main()
