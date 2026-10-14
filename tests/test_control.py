"""
The http control surface over a running compute: listing, lookup,
eviction, and the capacity, metrics and progress summaries.
"""

import time

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("plyght.web")

from fastapi.testclient import TestClient

from asteroidbelt import SyncCompute, ThreadCompute, build_control


def work(value):
    return value * 2


def slow(value):
    time.sleep(0.5)
    return value


@pytest.fixture
def compute():
    compute = SyncCompute(ThreadCompute(workers=2, sweep=0.05))
    compute.start()
    yield compute
    compute.close(drain=False)


@pytest.fixture
def client(compute):
    return TestClient(build_control(compute.compute))


def test_lists_and_finds_jobs(compute, client):
    compute.submit(work, name="double", arguments=(21,))
    assert compute.wait("double", timeout=5).value == 42
    names = [job["name"] for job in client.get("/api/v1/jobs").json()["jobs"]]
    assert names == ["double"]
    assert client.get("/api/v1/jobs/double").json()["status"] == "completed"
    assert client.get("/api/v1/jobs/missing").status_code == 404


def test_evicts_only_live_jobs(compute, client):
    compute.submit(slow, name="first", arguments=(1,))
    compute.submit(slow, name="also", arguments=(2,))
    compute.submit(slow, name="last", arguments=(3,))
    evicted = client.delete("/api/v1/jobs/last", params={"reason": "test"})
    assert evicted.json() == {"evicted": "last", "reason": "test"}
    assert client.delete("/api/v1/jobs/last").status_code == 409


def test_summaries(compute, client):
    compute.submit(work, name="one", arguments=(1,))
    compute.join(timeout=5)
    assert client.get("/api/v1/metrics").json()["jobs_completed_total"] == 1
    assert client.get("/api/v1/progress").json()["ratio"] == 1.0
    assert client.get("/api/v1/resources").json() == {"workers": 2, "running": 0, "queued": 0}
    assert client.get("/health").json() == {"status": "ok"}
