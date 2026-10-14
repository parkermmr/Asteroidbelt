"""
Shared directory durability: several compute instances backed by one
FileJournal directory recover each unfinished job exactly once, and a
crashed owner's jobs fail over once its lease goes stale.
"""

import asyncio
import time

from asteroidbelt import FileJournal, Job, PickleSerializer, ThreadCompute


def work(value):
    return value * 10


async def finished(compute, names):
    for _ in range(200):
        await compute.refresh()
        if all(name in compute.results for name in names):
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"jobs never finished: {names}")


def test_pending_job_claimed_by_one_instance(tmp_path):
    async def run():
        shared = tmp_path / "journal"
        writer = FileJournal(shared, instance="writer", lease=30.0)
        await writer.open()
        await writer.record("job-1", b"opaque")
        await writer.close()

        one = FileJournal(shared, instance="one", lease=30.0)
        two = FileJournal(shared, instance="two", lease=30.0)
        await one.open()
        await two.open()
        pending_one, _ = await one.replay()
        pending_two, _ = await two.replay()
        await one.close()
        await two.close()

        owners = [pending for pending in (pending_one, pending_two) if "job-1" in pending]
        assert len(owners) == 1

    asyncio.run(run())


def test_fresh_lease_not_stolen(tmp_path):
    async def run():
        shared = tmp_path / "journal"
        owner = FileJournal(shared, instance="owner", lease=30.0)
        await owner.open()
        await owner.record("job-1", b"opaque")

        peer = FileJournal(shared, instance="peer", lease=30.0)
        await peer.open()
        pending, _ = await peer.replay()
        assert "job-1" not in pending

        await owner.close()
        await peer.close()

    asyncio.run(run())


def test_stale_lease_is_stolen(tmp_path):
    async def run():
        shared = tmp_path / "journal"
        dead = FileJournal(shared, instance="dead", lease=0.2)
        await dead.open()
        await dead.record("job-1", b"opaque")
        await dead.close()

        time.sleep(0.3)

        survivor = FileJournal(shared, instance="survivor", lease=30.0)
        await survivor.open()
        pending, _ = await survivor.replay()
        assert "job-1" in pending
        assert survivor.read_lease("job-1")["owner"] == "survivor"
        await survivor.close()

    asyncio.run(run())


def test_shared_pool_runs_pending_once(tmp_path):
    async def run():
        shared = tmp_path / "journal"
        seed = FileJournal(shared, instance="seed", lease=30.0)
        await seed.open()
        job = Job(name="job-9", function=work, arguments=(7,))
        await seed.record("job-9", PickleSerializer().dumps(job))
        await seed.close()

        pool = [
            ThreadCompute(
                workers=2, journal=FileJournal(shared, instance=name, lease=30.0)
            )
            for name in ("a", "b", "c")
        ]
        for compute in pool:
            await compute.start()

        holders = [c for c in pool if "job-9" in c.records or "job-9" in c.results]
        assert len(holders) == 1
        await finished(holders[0], ["job-9"])
        result = await holders[0].poll("job-9")
        assert result.successful() and result.value == 70

        for compute in pool:
            await compute.stop()

    asyncio.run(run())


def test_tender_keeps_lease_fresh(tmp_path):
    async def run():
        shared = tmp_path / "journal"
        owner = FileJournal(shared, instance="owner", lease=0.3)
        await owner.open()
        await owner.record("job-1", b"opaque")
        first = owner.read_lease("job-1")["expiry"]
        await asyncio.sleep(0.5)
        second = owner.read_lease("job-1")["expiry"]
        assert second > first
        await owner.close()

    asyncio.run(run())
