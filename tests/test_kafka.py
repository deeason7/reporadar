from __future__ import annotations

import asyncio
from typing import Any

import pytest

from reporadar.config import Settings
from reporadar.github.events import RawEvent
from reporadar.ingest import kafka
from reporadar.ingest.kafka import KafkaSink, kafka_sink
from reporadar.ingest.wire import decode_value


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
