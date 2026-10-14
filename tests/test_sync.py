"""
SyncCompute: the blocking facade over an async Compute.
"""

from asteroidbelt import SyncCompute, ThreadCompute


def work(value):
    return value + 1


def test_sync_lifecycle():
    compute = SyncCompute(ThreadCompute(workers=2))
    compute.start()
    try:
        assert compute.submit(work, name="job-1", arguments=(41,))
        result = compute.wait("job-1", timeout=10.0)
        assert result.successful() and result.value == 42
        assert compute.offset() == 1
        assert compute.next().name == "job-1"
        assert compute.commit() == 1
        assert compute.metrics()["jobs_completed_total"] == 1
    finally:
        compute.stop()


def test_sync_context_manager():
    with SyncCompute(ThreadCompute(workers=1)) as compute:
        compute.submit(work, name="job-2", arguments=(1,))
        assert compute.wait("job-2", timeout=10.0).value == 2
        assert compute.find("job-2") is not None
        assert compute.progress()["completed"] == 1
