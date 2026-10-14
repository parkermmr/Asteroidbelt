"""
Journal durability: write ahead records, crash recovery, commit
acknowledgement, and claim check storage.
"""

import asyncio
import time

import pytest

from asteroidbelt import (
    FileJournal,
    FileStore,
    Job,
    KafkaJournal,
    PickleSerializer,
    ThreadCompute,
)


def work(value):
    return value * 10


def slow(value):
    time.sleep(0.3)
    return value


async def finished(compute, names):
    """
    Wait until every named job has settled.
    """
    for _ in range(200):
        await compute.refresh()
        if all(name in compute.results for name in names):
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"jobs never finished: {names}")


def test_file_journal_roundtrip(tmp_path):
    async def run():
        journal = FileJournal(tmp_path / "journal")
        await journal.open()
        await journal.record("a", b"job-a")
        await journal.record("b", b"job-b")
        await journal.settle("a", b"result-a")
        pending, settled = await journal.replay()
        assert pending == {"b": b"job-b"}
        assert settled == {"a": b"result-a"}
        await journal.forget("a")
        pending, settled = await journal.replay()
        assert pending == {"b": b"job-b"}
        assert settled == {}

    asyncio.run(run())


def test_results_survive_restart(tmp_path):
    async def run():
        compute = ThreadCompute(workers=2, journal=FileJournal(tmp_path / "journal"))
        await compute.start()
        await compute.submit(work, name="job-1", arguments=(3,))
        await finished(compute, ["job-1"])
        await compute.stop()

        revived = ThreadCompute(workers=2, journal=FileJournal(tmp_path / "journal"))
        await revived.start()
        result = await revived.poll("job-1")
        assert result.successful() and result.value == 30
        assert revived.offset() == 1
        assert (await revived.next()).name == "job-1"
        revived.commit()
        await revived.stop()

        third = ThreadCompute(workers=2, journal=FileJournal(tmp_path / "journal"))
        await third.start()
        with pytest.raises(KeyError):
            await third.poll("job-1")
        await third.stop()

    asyncio.run(run())


def test_pending_job_resubmitted(tmp_path):
    async def run():
        journal = FileJournal(tmp_path / "journal")
        await journal.open()
        job = Job(name="job-9", function=work, arguments=(7,))
        await journal.record("job-9", PickleSerializer().dumps(job))
        compute = ThreadCompute(workers=2, journal=journal)
        await compute.start()
        await finished(compute, ["job-9"])
        result = await compute.poll("job-9")
        assert result.successful() and result.value == 70
        await compute.stop()

    asyncio.run(run())


def test_job_live_at_stop_reruns_after_restart(tmp_path):
    async def run():
        compute = ThreadCompute(workers=1, journal=FileJournal(tmp_path / "journal"))
        await compute.start()
        await compute.submit(slow, name="job-5", arguments=(5,))
        await compute.stop()

        revived = ThreadCompute(workers=1, journal=FileJournal(tmp_path / "journal"))
        await revived.start()
        result = await revived.wait("job-5", timeout=10.0)
        assert result.successful() and result.value == 5
        await revived.stop()

    asyncio.run(run())


def test_unserializable_submit_fails_loudly(tmp_path):
    async def run():
        compute = ThreadCompute(workers=1, journal=FileJournal(tmp_path / "journal"))
        await compute.start()
        try:
            with pytest.raises(Exception):
                await compute.submit(lambda: 1, name="job-l")
            assert compute.find("job-l") is None
        finally:
            await compute.stop()

    asyncio.run(run())


def test_claim_check_seal_and_resolve(tmp_path):
    async def run():
        store = FileStore(tmp_path / "store")
        await store.open()
        journal = KafkaJournal("kafka:9092", "journal", store=store, limit=8)

        small = await journal.seal("job", "tiny", b"abc")
        assert small["data"] and "ref" not in small
        assert await journal.resolve(small) == b"abc"

        big = await journal.seal("result", "job/1", b"x" * 64)
        assert big["ref"] == "job%2F1.result" and "data" not in big
        assert await journal.resolve(big) == b"x" * 64

        await store.delete("job%2F1.result")
        assert await journal.resolve(big) is None

    asyncio.run(run())
