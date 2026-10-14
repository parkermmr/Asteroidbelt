"""
Bundle ships a function together with the files it reads: the archive
travels in the pickle, unpacks into a temporary directory passed as
the function's first argument, and is torn down afterwards.
"""

import asyncio
import time
from pathlib import Path

import pytest

from asteroidbelt import Bundle, PickleSerializer, ThreadCompute


def read_config(room):
    return Path(room, "config.txt").read_text()


def slow_read(room):
    time.sleep(0.2)
    return Path(room, "config.txt").read_text()


def read_with_arg(room, suffix):
    return Path(room, "config.txt").read_text() + suffix


class Reader:
    """
    A bundle aware method: the unpacked directory arrives after self.
    """

    def __init__(self, label):
        self.label = label

    def run(self, room):
        return self.label + ":" + Path(room, "config.txt").read_text()


def make_files(tmp_path):
    source = tmp_path / "config.txt"
    source.write_text("hello bundle")
    return [str(source)]


def test_bundle_passes_dir_as_first_argument(tmp_path):
    bundle = Bundle(read_config, files=make_files(tmp_path))
    assert bundle() == "hello bundle"


def test_bundle_forwards_further_arguments(tmp_path):
    bundle = Bundle(read_with_arg, files=make_files(tmp_path))
    assert bundle("!") == "hello bundle!"


def test_bundle_survives_pickle(tmp_path):
    bundle = Bundle(read_config, files=make_files(tmp_path))
    codec = PickleSerializer()
    restored = codec.loads(codec.dumps(bundle))
    assert restored() == "hello bundle"


def test_wrap_reuses_archive_and_binds(tmp_path):
    bundle = Bundle(files=make_files(tmp_path))
    bound = bundle.wrap(read_config)
    assert bound.archive is bundle.archive
    assert bound() == "hello bundle"


def test_unbound_bundle_refuses_to_run(tmp_path):
    bundle = Bundle(files=make_files(tmp_path))
    with pytest.raises(ValueError):
        bundle()


def test_bound_method_receives_dir_after_self(tmp_path):
    reader = Reader("r1")
    bundle = Bundle(reader.run, files=make_files(tmp_path))
    assert bundle() == "r1:hello bundle"
    codec = PickleSerializer()
    restored = codec.loads(codec.dumps(bundle))
    assert restored() == "r1:hello bundle"


def test_submit_files_bundles_internally(tmp_path):
    async def run():
        compute = ThreadCompute(workers=1)
        await compute.start()
        await compute.submit(read_config, name="job-f", files=make_files(tmp_path))
        result = await compute.wait("job-f", timeout=10.0)
        assert result.successful() and result.value == "hello bundle"
        await compute.stop()

    asyncio.run(run())


def test_submit_bundle_argument_reuses_archive(tmp_path):
    async def run():
        compute = ThreadCompute(workers=2)
        await compute.start()
        bundle = Bundle(files=make_files(tmp_path))
        await compute.submit(read_config, name="job-a", bundle=bundle)
        await compute.submit(read_config, name="job-b", bundle=bundle)
        first = await compute.wait("job-a", timeout=10.0)
        second = await compute.wait("job-b", timeout=10.0)
        assert first.value == "hello bundle" and second.value == "hello bundle"
        await compute.stop()

    asyncio.run(run())


def test_concurrent_bundle_jobs_stay_isolated(tmp_path):
    async def run():
        one = tmp_path / "one"
        one.mkdir()
        (one / "config.txt").write_text("one")
        two = tmp_path / "two"
        two.mkdir()
        (two / "config.txt").write_text("two")

        compute = ThreadCompute(workers=4)
        await compute.start()
        await compute.submit(slow_read, name="one", files=[str(one / "config.txt")])
        await compute.submit(slow_read, name="two", files=[str(two / "config.txt")])
        first = await compute.wait("one", timeout=10.0)
        second = await compute.wait("two", timeout=10.0)
        assert first.value == "one" and second.value == "two"
        await compute.stop()

    asyncio.run(run())


def test_submit_rejects_files_and_bundle_together(tmp_path):
    async def run():
        compute = ThreadCompute(workers=1)
        await compute.start()
        with pytest.raises(ValueError):
            await compute.submit(
                read_config,
                name="job-x",
                files=make_files(tmp_path),
                bundle=Bundle(files=make_files(tmp_path)),
            )
        await compute.stop()

    asyncio.run(run())
