"""
Codecs: pickle for module level functions, cloudpickle for closures
and lambdas, and Arrow for columnar tables with a cloudpickle
fallback for everything else.
"""

import importlib.util

import pytest

from asteroidbelt import ArrowSerializer, CloudSerializer, PickleSerializer

HAS_CLOUD = importlib.util.find_spec("cloudpickle") is not None
HAS_ARROW = importlib.util.find_spec("pyarrow") is not None


def top(value):
    return value + 1


def test_pickle_roundtrips_function_and_args():
    codec = PickleSerializer()
    restored = codec.loads(codec.dumps((top, (5,))))
    function, arguments = restored
    assert function(*arguments) == 6


@pytest.mark.skipif(not HAS_CLOUD, reason="cloudpickle not installed")
def test_cloud_roundtrips_a_closure():
    factor = 3

    def scale(value):
        return value * factor

    codec = CloudSerializer()
    restored = codec.loads(codec.dumps(scale))
    assert restored(10) == 30


@pytest.mark.skipif(not HAS_CLOUD, reason="cloudpickle not installed")
def test_cloud_roundtrips_a_lambda():
    codec = CloudSerializer()
    restored = codec.loads(codec.dumps(lambda value: value**2))
    assert restored(4) == 16


@pytest.mark.skipif(not HAS_ARROW, reason="pyarrow not installed")
def test_arrow_roundtrips_a_table():
    import pyarrow

    codec = ArrowSerializer()
    table = pyarrow.table({"n": [1, 2, 3], "name": ["a", "b", "c"]})
    restored = codec.loads(codec.dumps(table))
    assert restored.equals(table)


@pytest.mark.skipif(not HAS_ARROW, reason="pyarrow not installed")
def test_arrow_falls_back_for_functions():
    codec = ArrowSerializer()
    restored = codec.loads(codec.dumps((top, (9,))))
    function, arguments = restored
    assert function(*arguments) == 10
