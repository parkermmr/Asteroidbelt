"""
asteroidbelt - named jobs on arbitrary compute.

Submit any picklable function as a named job, poll for its result,
and let the watchdog enforce wall time budgets. LocalCompute runs
jobs as local processes, ThreadCompute on a thread pool, and
RayCompute delegates to a Ray cluster.

Usage:

    compute = LocalCompute()
    await compute.start()
    await compute.submit(work, name="job-1", cpus=2.0, ttl=60.0)
    result = await compute.poll("job-1")

or consume results queue style:

    result = await compute.next()
    compute.commit()

Attach a journal so submissions and results survive crashes and
power loss; start replays it, resubmitting unfinished jobs. A
FileJournal directory may be shared by a pool of instances, which
lease each job so exactly one recovers it and a dead owner fails
over:

    compute = LocalCompute(journal=FileJournal("/var/lib/asteroidbelt"))
    compute = RayCompute(
        journal=KafkaJournal("kafka:9092", "jobs", store=S3Store("results"))
    )

Pick a codec for what the jobs carry: PickleSerializer for module
level functions, CloudSerializer for closures and lambdas, and
ArrowSerializer for columnar tables. Name the files or binaries a
function reads and they are shipped with it, unpacked into a
directory passed as the function's first argument:

    def train(room, epochs):
        model = pathlib.Path(room, "model.bin").read_bytes()
        ...

    compute = ThreadCompute(serializer=CloudSerializer())
    compute.submit(train, name="job-1", arguments=(50,), files=["model.bin"])

or work without an event loop through the blocking facade:

    with SyncCompute(LocalCompute()) as compute:
        compute.submit(work, name="job-1")
        result = compute.wait("job-1", timeout=60.0)
"""

from asteroidbelt.codec.arrow_serializer import ArrowSerializer
from asteroidbelt.codec.cloud_serializer import CloudSerializer
from asteroidbelt.codec.pickle_serializer import PickleSerializer
from asteroidbelt.codec.serializer import Serializer
from asteroidbelt.compute.compute import Compute
from asteroidbelt.compute.local_compute import LocalCompute
from asteroidbelt.compute.ray_compute import RayCompute
from asteroidbelt.compute.sync_compute import SyncCompute
from asteroidbelt.compute.thread_compute import ThreadCompute
from asteroidbelt.control.control import build_control
from asteroidbelt.journal.file_journal import FileJournal
from asteroidbelt.journal.file_store import FileStore
from asteroidbelt.journal.journal import Journal
from asteroidbelt.journal.kafka_journal import KafkaJournal
from asteroidbelt.journal.s3_store import S3Store
from asteroidbelt.journal.store import Store
from asteroidbelt.models.bundle import Bundle
from asteroidbelt.models.job import Job
from asteroidbelt.models.job_event import JobEvent
from asteroidbelt.models.job_record import JobRecord
from asteroidbelt.models.job_result import JobResult
from asteroidbelt.models.job_status import JobStatus

__all__ = [
    "ArrowSerializer",
    "Bundle",
    "CloudSerializer",
    "Compute",
    "FileJournal",
    "FileStore",
    "Job",
    "JobEvent",
    "JobRecord",
    "JobResult",
    "JobStatus",
    "Journal",
    "KafkaJournal",
    "LocalCompute",
    "PickleSerializer",
    "RayCompute",
    "S3Store",
    "Serializer",
    "Store",
    "SyncCompute",
    "ThreadCompute",
    "build_control",
]
