"""
Bundle carries a function together with the files it needs at run.
"""

import io
import tarfile
import tempfile
from pathlib import Path
from typing import Any, Callable


class Bundle:
    """
    A callable that ships data or binaries alongside the function.

    Name the files and directories the job reads at submission; the
    serializer packs them into the payload, so a worker on another
    host needs nothing staged in advance. At call time the archive
    unpacks into a fresh temporary directory, the wrapped function
    runs with that directory as its first argument, and the directory
    is removed afterwards. The function reads its files from under
    that directory, each stored by its base name; nothing global like
    the process working directory is touched, so bundled jobs run
    concurrently without interfering. A bound method receives the
    directory after self, so write it def run(self, room, ...).

    Reach for submit's files argument for the common case; construct
    a Bundle directly to pack an archive once and reuse it across
    many submissions, either wrapping a function, Bundle(train,
    files=[...]), or handing a function free bundle to submit.

    The archive travels in the pickle, so keep it to the modest
    sidecars a function needs, not large datasets; those belong in a
    Store as a claim check.
    """

    def __init__(
        self,
        function: Callable | None = None,
        files: list[str] | None = None,
        archive: bytes | None = None,
    ) -> None:
        self.function = function
        self.files = files if files is not None else []
        self.archive = archive if archive is not None else self.pack(self.files)

    def wrap(self, function: Callable) -> "Bundle":
        """
        A copy of this bundle bound to a function, reusing the packed
        archive so the files are read from disk only once.
        """
        return Bundle(function=function, files=self.files, archive=self.archive)

    def pack(self, files: list[str]) -> bytes:
        """
        Read every named file or directory into a gzip tar archive,
        each stored under its own base name.
        """
        sink = io.BytesIO()
        with tarfile.open(fileobj=sink, mode="w:gz") as tar:
            for entry in files:
                source = Path(entry)
                tar.add(source, arcname=source.name)
        return sink.getvalue()

    def unpack(self, target: Path) -> None:
        """
        Expand the archive into a directory, preserving file modes so
        a bundled binary stays executable.
        """
        with tarfile.open(fileobj=io.BytesIO(self.archive), mode="r:gz") as tar:
            tar.extractall(target, filter="data")

    def __call__(self, *arguments: Any, **keywords: Any) -> Any:
        """
        Materialize the archive into a fresh temporary directory, run
        the function with that directory as its first argument, then
        remove it.
        """
        if self.function is None:
            raise ValueError("bundle has no function to run")
        with tempfile.TemporaryDirectory(prefix="bundle-") as room:
            self.unpack(Path(room))
            return self.function(Path(room), *arguments, **keywords)
