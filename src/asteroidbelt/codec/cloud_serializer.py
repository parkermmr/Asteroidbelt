"""
CloudSerializer encodes with cloudpickle for closures and lambdas.
"""

from typing import Any

from asteroidbelt.codec.serializer import Serializer


class CloudSerializer(Serializer):
    """
    Codec for functions the standard pickle refuses: lambdas,
    closures, locally defined functions, and code interactively
    typed at a repl. cloudpickle captures the referenced code by
    value, so the worker needs no shared import path for it. loads
    stays on the standard pickle, which reads any cloudpickle blob.
    Requires the cloudpickle package.
    """

    def dumps(self, value: Any) -> bytes:
        import cloudpickle

        return cloudpickle.dumps(value)

    def loads(self, blob: bytes) -> Any:
        import pickle

        return pickle.loads(blob)
