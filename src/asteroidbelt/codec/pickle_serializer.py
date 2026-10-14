"""
PickleSerializer encodes with the standard library pickle.
"""

import pickle
from typing import Any

from asteroidbelt.codec.serializer import Serializer


class PickleSerializer(Serializer):
    """
    Default codec. Pickle covers module level functions and bound
    methods whose instance is picklable, arguments included; for
    lambdas and closures reach for CloudSerializer instead.
    """

    def dumps(self, value: Any) -> bytes:
        return pickle.dumps(value)

    def loads(self, blob: bytes) -> Any:
        return pickle.loads(blob)
