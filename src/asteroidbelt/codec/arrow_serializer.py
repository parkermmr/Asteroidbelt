"""
ArrowSerializer encodes tabular data as an Arrow IPC stream.
"""

from typing import Any

from asteroidbelt.codec.cloud_serializer import CloudSerializer
from asteroidbelt.codec.serializer import Serializer

ARROW = b"A"
OTHER = b"P"


class ArrowSerializer(Serializer):
    """
    Columnar codec for pyarrow Tables and RecordBatches, which it
    writes as a self describing Arrow IPC stream: compact on the
    wire and cheap to read back into columns. Anything that is not
    an Arrow table, a plain function or its arguments included,
    falls through to a cloudpickle backed codec, so the serializer
    still handles arbitrary jobs. A one byte tag on the front tells
    loads which path produced the blob. Requires the pyarrow
    package.
    """

    def __init__(self) -> None:
        self.fallback = CloudSerializer()

    def dumps(self, value: Any) -> bytes:
        import pyarrow

        if isinstance(value, (pyarrow.Table, pyarrow.RecordBatch)):
            table = (
                value
                if isinstance(value, pyarrow.Table)
                else pyarrow.Table.from_batches([value])
            )
            sink = pyarrow.BufferOutputStream()
            with pyarrow.ipc.new_stream(sink, table.schema) as writer:
                writer.write_table(table)
            return ARROW + sink.getvalue().to_pybytes()
        return OTHER + self.fallback.dumps(value)

    def loads(self, blob: bytes) -> Any:
        import pyarrow

        tag, body = blob[:1], blob[1:]
        if tag == ARROW:
            with pyarrow.ipc.open_stream(body) as reader:
                return reader.read_all()
        return self.fallback.loads(body)
