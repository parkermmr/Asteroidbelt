[![CI Status][ci-shield]][ci-url]
[![Coverage][coverage-shield]][coverage-url]
[![Contributors][contributors-shield]][contributors-url]
[![Forks][forks-shield]][forks-url]
[![Stargazers][stars-shield]][stars-url]
[![Issues][issues-shield]][issues-url]

# Asteroidbelt

Named jobs on whatever compute you have. You submit a function under a
unique name with a cpu budget and a wall time ttl, then poll for the
result by name or read finished results in order like a queue. A
watchdog evicts jobs that run past their ttl. An optional journal lets
submissions and results survive a crash, and the same contract runs on
local processes, a thread pool or a Ray cluster.

```
pip install asteroidbelt                 # local processes and threads, standard library
pip install "asteroidbelt[ray]"          # + RayCompute
pip install "asteroidbelt[kafka]"        # + KafkaJournal (aiokafka)
pip install "asteroidbelt[s3]"           # + S3Store claim checks (boto3)
pip install "asteroidbelt[serialize]"    # + CloudSerializer for closures and lambdas (cloudpickle)
pip install "asteroidbelt[arrow]"        # + ArrowSerializer for tables (pyarrow)
pip install "asteroidbelt[control]"      # + HTTP control surface (plyght[web])
pip install "asteroidbelt[all]"
```

## Usage

```python
import asyncio
from asteroidbelt import LocalCompute

def square(value):
    return value * value

async def main():
    compute = LocalCompute(cpus=4)
    await compute.start()
    await compute.submit(square, name="square-7", arguments=(7,), cpus=1.0, ttl=60.0)
    result = await compute.wait("square-7", timeout=30)
    print(result.status, result.value)          # JobStatus.COMPLETED 49
    await compute.stop()

asyncio.run(main())
```

Code without an event loop can use the blocking facade:

```python
from asteroidbelt import LocalCompute, SyncCompute

with SyncCompute(LocalCompute()) as compute:
    compute.submit(square, name="square-7", arguments=(7,))
    result = compute.wait("square-7", timeout=30)
```

Leaving the `with` block waits for live jobs to finish, unless an
exception is escaping, in which case they are evicted. Without a
context, call `start()` and then `close(drain=True)`.

## Backends

| Backend                                   | Runs jobs on                 | Evict / expire                                                   | `resources()`                                      |
|-------------------------------------------|------------------------------|------------------------------------------------------------------|----------------------------------------------------|
| `LocalCompute(cpus=None)`                 | one forked process per job   | kills the process                                                | `total_cpus`, `available_cpus`, `running`, `queued` |
| `ThreadCompute(workers=None)`             | a shared thread pool         | queued jobs cancel; running threads finish and the result is dropped | `workers`, `running`, `queued`                 |
| `RayCompute(address=None, namespace=None)` | a Ray cluster (`ray.remote`) | `ray.cancel(force=True)`                                         | Ray's cluster and available resources              |

- `LocalCompute` queues jobs in submission order until enough of the cpu
  budget is free. The budget defaults to the machine's cpu count. It
  forks, so it needs a POSIX system.
- `ThreadCompute` suits io bound work and free threaded Python builds.
  Its default is four workers per cpu.
- `RayCompute` hands queueing to Ray, so a job counts as running from
  the moment it is submitted. Ray reruns a task whose worker died up to
  `retries` times. `namespace` isolates one tenant's actors and named
  objects on a shared cluster.
- Every backend takes `sweep` (seconds between watchdog passes),
  `journal` and `serializer`.

## API

- `start()` / `stop()`. On the local backends `stop` evicts whatever is
  still live, while Ray tasks keep running on the cluster. To let work
  finish first, call `join()` before stopping.
- `submit(function, name, arguments=(), keywords=None, cpus=1.0, ttl=3600.0, files=None, bundle=None) -> bool` -
  returns `False` when the name is already queued or running, so a
  redelivered submission never runs twice.
- `poll(name) -> JobResult | None` - the terminal result, or `None`
  while the job is still live. Unknown names raise `KeyError`.
- `wait(name, timeout=None)` - block until the job settles, or return
  `None` at the timeout.
- `join(timeout=None) -> bool` - block until every live job has settled.
- `evict(name, reason="evicted") -> bool` - stop a queued or running job.
- `subscribe(observer)` - `observer(JobEvent)` is called on every
  transition (`queued`, `running`, `completed`, `failed`, `expired`,
  `evicted`). This is the hook for progress bars, persistence and
  metrics exporters.
- `jobs()`, `find(name)`, `progress()`, `metrics()`, `snapshot()` -
  serializable views. `metrics()` is flat counters and gauges, ready to
  scrape.

A `JobResult` has `name`, `status`, `value` and `error`, and
`successful()` is true only for `completed`. A job that raises settles
as `failed`, and `error` holds the exception's repr.

### Results as a queue

Finished results also land on a queue with Kafka style offsets:

```python
result = await compute.next()       # next unread result, or None
batch = await compute.poll(50)      # up to 50 unread results
compute.commit()                    # acknowledge everything read so far
compute.rollback()                  # or reread everything uncommitted
```

`offset()` is how many results are still unread. The newest 500
finished jobs are kept for `poll(name)` and `jobs()`.

## Journals

A journal records each submission before its launch and each result
after it settles. On `start`, results that were never committed become
readable again and jobs that never settled are resubmitted. Execution
and delivery are therefore at least once. `commit` removes acknowledged
results from the journal.

```python
from asteroidbelt import FileJournal, KafkaJournal, LocalCompute, RayCompute, S3Store

LocalCompute(journal=FileJournal("/var/lib/asteroidbelt"))
RayCompute(journal=KafkaJournal("kafka:9092", "jobs", store=S3Store("results")))
```

| Journal                                | Durability                                                                 |
|----------------------------------------|----------------------------------------------------------------------------|
| `FileJournal(path, lease=30.0)`        | fsynced files; one directory can be shared by a pool of computes            |
| `KafkaJournal(servers, topic, store=None, limit=512KiB)` | one keyed message per entry; replay reads the topic, so compaction is safe |

- `FileJournal` leases each job to the compute that owns it and renews
  the lease while it runs. When an owner dies, the next compute to
  replay takes over the expired lease. A pool sharing a directory
  therefore recovers each unfinished job exactly once.
- `KafkaJournal` sends payloads larger than `limit` to `store` as a
  claim check: `FileStore(path)`, or `S3Store(bucket, prefix="", **boto3_options)`.
  `cafile`, `certfile` and `keyfile` turn on TLS. Any other keyword is
  passed through to aiokafka.
- Everything a journal stores is serialized by the compute's
  `serializer`. `PickleSerializer` is the default and handles module
  level functions. `CloudSerializer` handles closures and lambdas, and
  `ArrowSerializer` writes pyarrow tables as Arrow IPC.

## Shipping files

Name the files or directories a function reads and they are packed into
the job. They are unpacked into a fresh temporary directory, which the
function receives as its first argument:

```python
def train(room, epochs):
    weights = Path(room, "model.bin").read_bytes()
    ...

await compute.submit(train, name="train-1", arguments=(50,), files=["model.bin"])
```

Each file is stored by its base name. Build a `Bundle(files=[...])` once
and pass `bundle=` to reuse one archive across many submissions. The
archive travels inside the job, so it suits small sidecar files. Large
datasets belong in a store.

## HTTP control

`asteroidbelt[control]` exposes a compute over HTTP, similar to
`squeue` and `scancel`:

```python
import uvicorn
from asteroidbelt import build_control

uvicorn.run(build_control(compute), port=8000)
```

| Route                         | Does                         |
|-------------------------------|------------------------------|
| `GET /api/v1/jobs`            | live jobs plus recent history |
| `GET /api/v1/jobs/{name}`     | one job, live or finished     |
| `DELETE /api/v1/jobs/{name}`  | evict; 409 when not live      |
| `GET /api/v1/resources`       | capacity                      |
| `GET /api/v1/metrics`         | counters and gauges           |
| `GET /api/v1/progress`        | progress bar summary          |

The app is built on `plyght.web`, so it includes `/health`, `/ready`,
`/docs` and the auth proxy middleware. Configure it with `ASTEROIDBELT_*`
variables (`ASTEROIDBELT_AUTH_ENABLED`, ...) or pass `settings=`.

## Behaviour notes

- Reaping runs on a private daemon thread with its own event loop. Jobs
  settle and expire even while the caller blocks its own loop, and all
  journal work runs on that thread's loop.
- A job's ttl counts from when it starts running, or from submission
  while it is still queued. Ray jobs count as running from submission.
- `stop()` does not mark live jobs as settled in the journal, so the
  next `start()` resubmits them.

## Samples

[`sample/example.py`](sample/example.py) walks through named jobs and
observers, the result queue with commit and rollback, recovery from a
journal after a restart, and shipping files with a job.

## Tests

```
pip install -e ".[kafka,s3,serialize,arrow,control,dev]"
pytest --cov
```

[ci-shield]: https://img.shields.io/github/actions/workflow/status/parkermmr/asteroidbelt/ci.yml?branch=main&style=for-the-badge&label=CI
[ci-url]: https://github.com/parkermmr/asteroidbelt/actions/workflows/ci.yml
[coverage-shield]: https://img.shields.io/codecov/c/github/parkermmr/asteroidbelt?style=for-the-badge
[coverage-url]: https://codecov.io/gh/parkermmr/asteroidbelt
[contributors-shield]: https://img.shields.io/github/contributors/parkermmr/asteroidbelt.svg?style=for-the-badge
[contributors-url]: https://github.com/parkermmr/asteroidbelt/graphs/contributors
[forks-shield]: https://img.shields.io/github/forks/parkermmr/asteroidbelt.svg?style=for-the-badge
[forks-url]: https://github.com/parkermmr/asteroidbelt/network/members
[stars-shield]: https://img.shields.io/github/stars/parkermmr/asteroidbelt.svg?style=for-the-badge
[stars-url]: https://github.com/parkermmr/asteroidbelt/stargazers
[issues-shield]: https://img.shields.io/github/issues/parkermmr/asteroidbelt.svg?style=for-the-badge
[issues-url]: https://github.com/parkermmr/asteroidbelt/issues
