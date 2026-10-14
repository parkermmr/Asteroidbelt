"""
KafkaJournal persists jobs and results on a Kafka topic.
"""

import json
from base64 import b64decode, b64encode
from urllib.parse import quote

from asteroidbelt.journal.journal import Journal
from asteroidbelt.journal.store import Store

LIMIT = 512 * 1024


class KafkaJournal(Journal):
    """
    Broker backed journal for durability beyond one machine. Every
    entry is one message keyed by job name; replay reads the topic
    from the beginning and folds it into pending jobs and settled
    results, so a compacted topic stays correct. With a store
    attached, payloads over limit bytes travel as a claim check:
    the blob goes to the store and the message carries only its
    key.
    """

    def __init__(
        self,
        servers: str,
        topic: str,
        store: Store | None = None,
        limit: int = LIMIT,
        cafile: str | None = None,
        certfile: str | None = None,
        keyfile: str | None = None,
        password: str | None = None,
        **options,
    ) -> None:
        self.servers = servers
        self.topic = topic
        self.store = store
        self.limit = limit
        self.cafile = cafile
        self.certfile = certfile
        self.keyfile = keyfile
        self.password = password
        self.options = options
        self.producer = None

    def secure(self) -> dict:
        """
        Shared client settings: bootstrap servers, a TLS context
        built from the certificate files when given, then every
        extra option layered on top, overriding anything here.
        """
        settings: dict = {"bootstrap_servers": self.servers}
        if self.cafile is not None or self.certfile is not None:
            from aiokafka.helpers import create_ssl_context

            settings["ssl_context"] = create_ssl_context(
                cafile=self.cafile,
                certfile=self.certfile,
                keyfile=self.keyfile,
                password=self.password,
            )
            settings["security_protocol"] = "SSL"
        settings.update(self.options)
        return settings

    async def open(self) -> None:
        from aiokafka import AIOKafkaProducer

        self.producer = AIOKafkaProducer(**{"acks": "all", **self.secure()})
        await self.producer.start()
        if self.store is not None:
            await self.store.open()

    async def close(self) -> None:
        if self.producer is not None:
            await self.producer.stop()
            self.producer = None
        if self.store is not None:
            await self.store.close()

    async def seal(self, kind: str, name: str, blob: bytes) -> dict:
        """
        Build a message envelope, parking oversized payloads in the
        store and keeping only their key.
        """
        envelope = {"kind": kind, "name": name}
        if self.store is not None and len(blob) > self.limit:
            key = f"{quote(name, safe='')}.{kind}"
            await self.store.put(key, blob)
            envelope["ref"] = key
        else:
            envelope["data"] = b64encode(blob).decode()
        return envelope

    async def resolve(self, envelope: dict) -> bytes | None:
        """
        Recover the payload behind an envelope, following its claim
        check into the store when present.
        """
        if envelope.get("data") is not None:
            return b64decode(envelope["data"])
        reference = envelope.get("ref")
        if reference is None or self.store is None:
            return None
        return await self.store.get(reference)

    async def publish(self, envelope: dict) -> None:
        value = json.dumps(envelope).encode()
        await self.producer.send_and_wait(
            self.topic, value, key=envelope["name"].encode()
        )

    async def record(self, name: str, blob: bytes) -> None:
        await self.publish(await self.seal("job", name, blob))

    async def settle(self, name: str, blob: bytes) -> None:
        await self.publish(await self.seal("result", name, blob))

    async def forget(self, name: str) -> None:
        await self.publish({"kind": "forget", "name": name})
        if self.store is not None:
            await self.store.delete(f"{quote(name, safe='')}.job")
            await self.store.delete(f"{quote(name, safe='')}.result")

    async def replay(self) -> tuple[dict[str, bytes], dict[str, bytes]]:
        from aiokafka import AIOKafkaConsumer, TopicPartition

        consumer = AIOKafkaConsumer(
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            **self.secure(),
        )
        await consumer.start()
        jobs: dict[str, dict] = {}
        results: dict[str, dict] = {}
        try:
            numbers = consumer.partitions_for_topic(self.topic) or set()
            partitions = [TopicPartition(self.topic, number) for number in numbers]
            if partitions:
                consumer.assign(partitions)
                await consumer.seek_to_beginning(*partitions)
                ends = await consumer.end_offsets(partitions)
                await self.fold(consumer, partitions, ends, jobs, results)
        finally:
            await consumer.stop()
        pending = {}
        for name, envelope in jobs.items():
            if name in results:
                continue
            blob = await self.resolve(envelope)
            if blob is not None:
                pending[name] = blob
        settled = {}
        for name, envelope in results.items():
            blob = await self.resolve(envelope)
            if blob is not None:
                settled[name] = blob
        return pending, settled

    async def fold(self, consumer, partitions, ends, jobs, results) -> None:
        """
        Consume every existing message once, keeping the latest
        envelope per job and dropping forgotten ones.
        """
        while True:
            behind = False
            for partition in partitions:
                if await consumer.position(partition) < ends[partition]:
                    behind = True
            if not behind:
                return
            batches = await consumer.getmany(timeout_ms=500)
            for records in batches.values():
                for record in records:
                    envelope = json.loads(record.value)
                    kind = envelope.get("kind")
                    name = envelope.get("name")
                    if kind == "job":
                        jobs[name] = envelope
                    elif kind == "result":
                        results[name] = envelope
                    elif kind == "forget":
                        jobs.pop(name, None)
                        results.pop(name, None)
