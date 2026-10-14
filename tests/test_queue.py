"""
Queue style consumption of finished jobs: next, poll, find, commit,
rollback, offset, and pending.
"""

import asyncio

import pytest

from asteroidbelt import JobStatus, ThreadCompute


def work(value):
    return value * 10


def boom(value):
    raise RuntimeError(f"boom {value}")


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


def test_next_commit_rollback_offset():
    async def run():
        compute = ThreadCompute(workers=2)
        await compute.start()
        try:
            for index in range(3):
                await compute.submit(work, name=f"job-{index}", arguments=(index,))
            await finished(compute, [f"job-{index}" for index in range(3)])

            assert compute.offset() == 3
            first = await compute.next()
            assert first.successful() and first.value in (0, 10, 20)
            assert compute.offset() == 2

            assert compute.rollback() == 1
            assert compute.offset() == 3
            again = await compute.next()
            assert again.name == first.name

            rest = await compute.poll(10)
            assert len(rest) == 2
            assert compute.offset() == 0

            assert compute.commit() == 3
            assert compute.offset() == 0
            assert await compute.next() is None
        finally:
            await compute.stop()

    asyncio.run(run())


def test_poll_by_name_still_works():
    async def run():
        compute = ThreadCompute(workers=1)
        await compute.start()
        try:
            await compute.submit(work, name="named", arguments=(4,))
            await finished(compute, ["named"])
            result = await compute.poll("named")
            assert result.value == 40
            with pytest.raises(KeyError):
                await compute.poll("missing")
        finally:
            await compute.stop()

    asyncio.run(run())


def test_find_and_pending():
    async def run():
        compute = ThreadCompute(workers=1)
        await compute.start()
        try:
            await compute.submit(work, name="findable", arguments=(1,))
            record = compute.find("findable")
            assert record is not None and record.name == "findable"
            assert compute.find("nowhere") is None
            await finished(compute, ["findable"])
            assert compute.find("findable").status == JobStatus.COMPLETED
            assert compute.pending() == 0
        finally:
            await compute.stop()

    asyncio.run(run())


def test_failures_flow_through_queue():
    async def run():
        compute = ThreadCompute(workers=1)
        await compute.start()
        try:
            await compute.submit(boom, name="bad", arguments=(9,))
            await finished(compute, ["bad"])
            result = await compute.next()
            assert not result.successful()
            assert "boom 9" in result.error
        finally:
            await compute.stop()

    asyncio.run(run())
