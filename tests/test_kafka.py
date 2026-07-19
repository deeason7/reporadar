from __future__ import annotations

import asyncio
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import pytest

from reporadar.config import Settings
from reporadar.github.events import RawEvent
from reporadar.ingest import kafka
from reporadar.ingest.consumer import DeadLetter, consume_stream
from reporadar.ingest.kafka import KafkaSink, kafka_sink, kafka_source
from reporadar.ingest.wire import WireEnvelope, decode_value, encode_key, encode_value


class FakeProducer:
    """aiokafka-shaped double: send() records the message and hands back a
    delivery future the 'broker' has already settled — resolved, or failed for
    the (1-based) send indexes listed in ``fail_on``."""

    def __init__(self, **kwargs: object) -> None:
        self.init_kwargs = kwargs
        self.sent: list[tuple[str, bytes | None, bytes | None]] = []
        self.fail_on: set[int] = set()
        self.started = False
        self.stopped = False

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    async def send(
        self, topic: str, value: bytes | None = None, key: bytes | None = None
    ) -> asyncio.Future[object]:
        self.sent.append((topic, value, key))
        future: asyncio.Future[object] = asyncio.get_running_loop().create_future()
        if len(self.sent) in self.fail_on:
            future.set_exception(ConnectionError("broker went away"))
        else:
            future.set_result(object())  # stand-in for the record metadata
        return future


def _second_event(event_dict: dict[str, Any]) -> RawEvent:
    return RawEvent.model_validate(
        {**event_dict, "id": "45000000002", "repo": {**event_dict["repo"], "id": 7}}
    )


async def test_sink_produces_one_wire_message_per_event(event_dict: dict[str, Any]) -> None:
    producer = FakeProducer()
    first = RawEvent.model_validate(event_dict)
    second = _second_event(event_dict)

    await KafkaSink(producer, topic="raw.events.test")([first, second])

    assert [topic for topic, _, _ in producer.sent] == ["raw.events.test"] * 2
    assert [key for _, _, key in producer.sent] == [b"2", b"7"]  # repo ids, as decimal bytes
    envelopes = [decode_value(value) for _, value, _ in producer.sent if value is not None]
    assert [envelope.event.id for envelope in envelopes] == [first.id, second.id]
    assert envelopes[0].captured_at == envelopes[1].captured_at  # one batch, one capture instant


async def test_delivery_failure_surfaces_after_full_handoff(event_dict: dict[str, Any]) -> None:
    # The sink's contract is at-least-once per batch: it returns only once every
    # delivery is confirmed, so a failed confirmation must raise, not vanish.
    producer = FakeProducer()
    producer.fail_on = {2}
    events = [RawEvent.model_validate(event_dict) for _ in range(3)]

    with pytest.raises(ConnectionError):
        await KafkaSink(producer, topic="raw.events.test")(events)

    # All three sends happened before the barrier — the pipelining half of the design.
    assert len(producer.sent) == 3


async def test_factory_wires_settings_and_manages_the_producer(
    monkeypatch: pytest.MonkeyPatch, settings: Settings, event_dict: dict[str, Any]
) -> None:
    created: list[FakeProducer] = []

    def fake_producer_type(**kwargs: object) -> FakeProducer:
        producer = FakeProducer(**kwargs)
        created.append(producer)
        return producer

    monkeypatch.setattr(kafka, "AIOKafkaProducer", fake_producer_type)

    async with kafka_sink(settings) as sink:
        [producer] = created
        assert producer.started and not producer.stopped
        assert producer.init_kwargs["bootstrap_servers"] == settings.kafka_bootstrap_servers
        await sink([RawEvent.model_validate(event_dict)])

    assert producer.stopped  # exit stops (and thereby drains) the producer
    [(topic, _, _)] = producer.sent
    assert topic == settings.kafka_live_topic  # the configured topic, threaded through


async def test_factory_stops_the_producer_when_the_run_crashes(
    monkeypatch: pytest.MonkeyPatch, settings: Settings
) -> None:
    created: list[FakeProducer] = []

    def fake_producer_type(**kwargs: object) -> FakeProducer:
        producer = FakeProducer(**kwargs)
        created.append(producer)
        return producer

    monkeypatch.setattr(kafka, "AIOKafkaProducer", fake_producer_type)

    with pytest.raises(RuntimeError):
        async with kafka_sink(settings):
            raise RuntimeError("service crashed mid-run")

    assert created[0].stopped  # no connection leaks behind a crash


@dataclass
class FakeRecord:
    """The slice of a consumed record the source reads."""

    value: bytes | None
    key: bytes | None = None


class FakeConsumer:
    """aiokafka-shaped consumer double: serves the given batches in order, then
    empty polls forever (a real topic never ends), and records every commit."""

    def __init__(self, *batches: Sequence[FakeRecord], **kwargs: object) -> None:
        self.init_kwargs = kwargs
        self._batches = iter(batches)
        self.polls = 0
        self.commits = 0
        self.started = False
        self.stopped = False

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    async def getmany(self, *, timeout_ms: int) -> Mapping[object, Sequence[FakeRecord]]:
        self.polls += 1
        batch = next(self._batches, ())
        if not batch:
            # A real idle poll parks for up to timeout_ms before returning empty.
            # Suspending here keeps an idle consume loop cancellable — an instant
            # empty return would spin it hot, beyond the reach of any timeout.
            await asyncio.sleep(timeout_ms / 1000)
            return {}
        return {"partition-0": batch}

    async def commit(self) -> None:
        self.commits += 1


def _wire_record(event_dict: dict[str, Any], id_: str) -> FakeRecord:
    event = RawEvent.model_validate({**event_dict, "id": id_})
    return FakeRecord(value=encode_value(event), key=encode_key(event))


def _install(monkeypatch: pytest.MonkeyPatch, consumer: FakeConsumer) -> None:
    monkeypatch.setattr(kafka, "AIOKafkaConsumer", lambda *a, **k: consumer)


async def test_source_yields_consumed_messages_with_their_bytes(
    monkeypatch: pytest.MonkeyPatch, settings: Settings, event_dict: dict[str, Any]
) -> None:
    consumer = FakeConsumer([_wire_record(event_dict, "a"), _wire_record(event_dict, "b")])
    _install(monkeypatch, consumer)

    async with kafka_source(settings, timeout_ms=5) as source:
        batch = await anext(aiter(source))

    assert [decode_value(message.value).event.id for message in batch] == ["a", "b"]
    assert [message.key for message in batch] == [b"2", b"2"]  # repo-id keys ride along


async def test_offsets_commit_only_once_the_loop_comes_back_for_more(
    monkeypatch: pytest.MonkeyPatch, settings: Settings, event_dict: dict[str, Any]
) -> None:
    # The heart of store-then-commit through an iterator seam: the generator
    # resumes only after the consume loop has stored what it was handed, so
    # holding a batch must leave its offsets uncommitted.
    consumer = FakeConsumer([_wire_record(event_dict, "a")], [_wire_record(event_dict, "b")])
    _install(monkeypatch, consumer)

    async with kafka_source(settings, timeout_ms=5) as source:
        batches = aiter(source)
        await anext(batches)
        assert consumer.commits == 0  # batch 1 is out with the loop — nothing durable yet

        await anext(batches)  # coming back for more == batch 1 was stored
        assert consumer.commits == 1


async def test_an_unfinished_batch_is_never_committed(
    monkeypatch: pytest.MonkeyPatch, settings: Settings, event_dict: dict[str, Any]
) -> None:
    # A batch the loop never finished (crash, or a stop after the last batch) must
    # stay uncommitted so it is redelivered — the store's idempotent insert absorbs
    # the duplicate. Committing here instead would be at-most-once: silent loss.
    consumer = FakeConsumer([_wire_record(event_dict, "a")])
    _install(monkeypatch, consumer)

    async with kafka_source(settings, timeout_ms=5) as source:
        await anext(aiter(source))  # take a batch and walk away

    assert consumer.commits == 0
    assert consumer.stopped


async def test_an_idle_poll_yields_an_empty_batch_and_commits_nothing(
    monkeypatch: pytest.MonkeyPatch, settings: Settings
) -> None:
    # The empty batch is load-bearing: it hands control back so the consume loop
    # can check its stop event instead of blocking until traffic appears.
    consumer = FakeConsumer()
    _install(monkeypatch, consumer)

    async with kafka_source(settings, timeout_ms=5) as source:
        batches = aiter(source)
        assert list(await anext(batches)) == []
        await anext(batches)

    assert consumer.commits == 0  # nothing arrived, so there is nothing to commit
    assert consumer.polls == 2


async def test_a_null_valued_message_is_dead_lettered_not_skipped(
    monkeypatch: pytest.MonkeyPatch, settings: Settings
) -> None:
    # A tombstone is not something this pipeline writes, so it is a foreign write.
    # It must be visible as undecodable, never silently dropped.
    consumer = FakeConsumer([FakeRecord(value=None)])
    _install(monkeypatch, consumer)

    async with kafka_source(settings, timeout_ms=5) as source:
        [message] = await anext(aiter(source))

    assert message.value == b""  # empty bytes fail to decode -> the DLQ sees them


async def test_factory_wires_settings_and_manages_the_consumer(
    monkeypatch: pytest.MonkeyPatch, settings: Settings
) -> None:
    created: list[FakeConsumer] = []

    def fake_consumer_type(*topics: str, **kwargs: object) -> FakeConsumer:
        consumer = FakeConsumer(**{**kwargs, "topics": topics})
        created.append(consumer)
        return consumer

    monkeypatch.setattr(kafka, "AIOKafkaConsumer", fake_consumer_type)

    async with kafka_source(settings):
        [consumer] = created
        assert consumer.started and not consumer.stopped

    assert consumer.init_kwargs["topics"] == (settings.kafka_live_topic,)
    assert consumer.init_kwargs["bootstrap_servers"] == settings.kafka_bootstrap_servers
    # Auto-commit off is the whole design: offsets record what was stored, not
    # what was delivered, and only the batch generator knows the difference.
    assert consumer.init_kwargs["enable_auto_commit"] is False
    assert consumer.init_kwargs["group_id"] == kafka.CONSUMER_GROUP
    assert consumer.stopped


async def test_store_then_commit_end_to_end_through_the_consume_loop(
    monkeypatch: pytest.MonkeyPatch, settings: Settings, event_dict: dict[str, Any]
) -> None:
    # The real loop, the real source: offsets must trail the store by exactly one
    # batch, and a store that raises must leave its batch uncommitted.
    consumer = FakeConsumer([_wire_record(event_dict, "a")], [_wire_record(event_dict, "b")])
    _install(monkeypatch, consumer)
    commits_seen_at_store: list[int] = []

    async def store(envelopes: Sequence[WireEnvelope]) -> None:
        commits_seen_at_store.append(consumer.commits)
        if envelopes[0].event.id == "b":
            raise ConnectionError("database went away")

    async def dead_letter(letters: Sequence[DeadLetter]) -> None:
        raise AssertionError("nothing here fails to decode")

    async with kafka_source(settings, timeout_ms=5) as source:
        # Bounded: the fake, like a real topic, never ends. If the exit exception
        # is starved (a bug upstream of the store), this must fail fast, not hang.
        async with asyncio.timeout(5):
            with pytest.raises(ConnectionError):
                await consume_stream(source, store, dead_letter)

    # Storing "a" saw zero commits; storing "b" saw exactly one (a's), proving the
    # commit landed after a's store and before b's.
    assert commits_seen_at_store == [0, 1]
    # b's store raised, so the loop never came back and b stays uncommitted —
    # it will be redelivered, and the idempotent store will absorb it.
    assert consumer.commits == 1
