"""
FileStore keeps blobs as files in one directory.
"""

from asyncio import to_thread
from os import fsync
from pathlib import Path
from urllib.parse import quote

from asteroidbelt.journal.store import Store


class FileStore(Store):
    """
    Claim check storage on a local or shared filesystem. Writes land
    in a temporary file, are fsynced, then renamed onto the key, so a
    reader on a shared mount never sees a half written blob and a
    crash never leaves a torn one.
    """

    def __init__(self, path: str) -> None:
        self.path = Path(path)

    async def open(self) -> None:
        self.path.mkdir(parents=True, exist_ok=True)

    def locate(self, key: str) -> Path:
        return self.path / quote(key, safe=".")

    def store(self, key: str, blob: bytes) -> None:
        """
        Atomic durable write: temporary file, fsync, rename.
        """
        target = self.locate(key)
        staging = target.with_name(target.name + ".tmp")
        with open(staging, "wb") as handle:
            handle.write(blob)
            handle.flush()
            fsync(handle.fileno())
        staging.replace(target)

    async def put(self, key: str, blob: bytes) -> None:
        await to_thread(self.store, key, blob)

    async def get(self, key: str) -> bytes | None:
        target = self.locate(key)
        if not target.exists():
            return None
        return await to_thread(target.read_bytes)

    async def delete(self, key: str) -> None:
        await to_thread(self.locate(key).unlink, missing_ok=True)
