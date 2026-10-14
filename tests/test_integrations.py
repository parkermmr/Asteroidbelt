"""
Integration surfaces: observer events for persistence, progress
summaries, metrics counters, and snapshots.
"""

import asyncio
from time import sleep

from asteroidbelt import JobStatus, LocalCompute, SyncCompute, ThreadCompute


def work(value):
    return value + 1


def slow(value):
    sleep(0.3)
    return value + 1


async def finished(compute, names):
    for _ in range(200):
        await compute.refresh()
        if all(name in compute.results for name in names):
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"jobs never finished: {names}")


def test_observers_see_every_transition():
    events = []

    async def run():
        compute = ThreadCompute(workers=1)
        compute.subscribe(lambda event: events.append(event))
        await compute.start()
        try:
            await compute.submit(work, name="observed", arguments=(1,))
            await finished(compute, ["observed"])
            await asyncio.sleep(0.02)
        finally:
            await compute.stop()

    asyncio.run(run())
    statuses = [event.status for event in events if event.name == "observed"]
    assert statuses[0] == JobStatus.QUEUED
    assert JobStatus.RUNNING in statuses
    assert statuses[-1] == JobStatus.COMPLETED
    assert all(event.describe()["name"] == "observed" for event in events)


def test_broken_observer_never_breaks_compute():
    async def run():
        compute = ThreadCompute(workers=1)
        compute.subscribe(lambda event: 1 / 0)
        await compute.start()
        try:
            await compute.submit(work, name="sturdy", arguments=(1,))
            await finished(compute, ["sturdy"])
            result = await compute.poll("sturdy")
            assert result.value == 2
        finally:
            await compute.stop()

    asyncio.run(run())


def test_progress_metrics_and_snapshot():
    async def run():
        compute = ThreadCompute(workers=2)
        await compute.start()
        try:
            for index in range(4):
                await compute.submit(work, name=f"job-{index}", arguments=(index,))
            await finished(compute, [f"job-{index}" for index in range(4)])

            progress = compute.progress()
            assert progress["total"] == 4
            assert progress["done"] == 4
            assert progress["ratio"] == 1.0

            metrics = compute.metrics()
            assert metrics["jobs_submitted_total"] == 4
            assert metrics["jobs_completed_total"] == 4
            assert metrics["jobs_failed_total"] == 0
            assert metrics["queue_unread"] == 4
            assert metrics["seconds_running_total"] >= 0

            snapshot = compute.snapshot()
            assert len(snapshot["unread"]) == 4
            assert snapshot["uncommitted"] == []
            assert len(snapshot["jobs"]) == 4
        finally:
            await compute.stop()

    asyncio.run(run())


def test_local_compute_still_settles_jobs():
    async def run():
        compute = LocalCompute(cpus=2.0)
        await compute.start()
        try:
            await compute.submit(work, name="proc", arguments=(41,))
            await finished(compute, ["proc"])
            result = await compute.poll("proc")
            assert result.value == 42
            assert compute.resources()["queued"] == 0
        finally:
            await compute.stop()

    asyncio.run(run())


def test_reaper_settles_while_the_driver_loop_is_blocked():
    """
    The reaper runs on its own thread, so jobs settle even when the
    caller never yields the event loop. Before this, a driver stuck
    in synchronous work left finished jobs unreaped forever.
    """

    async def run():
        compute = ThreadCompute(workers=4, sweep=0.1)
        await compute.start()
        try:
            for index in range(4):
                await compute.submit(slow, name=f"blocked-{index}", arguments=(index,))
            sleep(1.0)
            assert not compute.records
            assert len(compute.results) == 4
        finally:
            await compute.stop()

    asyncio.run(run())


def test_sync_close_drains_instead_of_evicting():
    """
    A fire and forget caller submits, walks away, and closes with a
    drain; every job finishes rather than being evicted at stop.
    """
    compute = SyncCompute(ThreadCompute(workers=4, sweep=0.1))
    compute.start()
    submitted = False
    try:
        for index in range(4):
            compute.submit(slow, name=f"ff-{index}", arguments=(index,))
        submitted = True
        assert compute.join(timeout=5.0)
        assert compute.metrics()["jobs_completed_total"] == 4
        assert compute.metrics()["jobs_evicted_total"] == 0
    finally:
        compute.close(drain=submitted)
