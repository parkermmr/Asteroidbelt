"""
Store is keyed blob storage for claim check payloads.
"""

import abc


class Store(abc.ABC):
    """
    Minimal key to bytes storage. Journals park oversized payloads
    here and put only the key on the broker.
    """

    async def open(self) -> None:
        """
        Prepare backing resources.
        """

    async def close(self) -> None:
        """
        Release backing resources.
        """

    @abc.abstractmethod
    async def put(self, key: str, blob: bytes) -> None:
        """
        Write a blob under a key, replacing any previous blob.
        """

    @abc.abstractmethod
    async def get(self, key: str) -> bytes | None:
        """
        Read a blob, or None when the key is absent.
        """

    @abc.abstractmethod
    async def delete(self, key: str) -> None:
        """
        Remove a key; absent keys are not an error.
        """
