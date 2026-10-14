"""
Serializer turns jobs and results into bytes for a journal.
"""

import abc
from typing import Any


class Serializer(abc.ABC):
    """
    Two way codec between objects and bytes. Journals store whatever
    dumps produces and hand it back to loads on replay, so the two
    sides of a restart must agree on the codec.
    """

    @abc.abstractmethod
    def dumps(self, value: Any) -> bytes:
        """
        Encode a value to bytes.
        """

    @abc.abstractmethod
    def loads(self, blob: bytes) -> Any:
        """
        Decode bytes back to a value.
        """
