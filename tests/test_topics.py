from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import pytest

from reporadar.config import Settings
from reporadar.ingest import topics
from reporadar.ingest.topics import (
    TopicOutcome,
    TopicSpec,
    ensure_topics,
    kafka_admin,
    provision_topics,
    require_topics,
    required_topics,
    verify_topics,
)


@dataclass
class FakeCreateResponse:
    topic_errors: list[tuple[Any, ...]]


class FakeAdmin:
    """aiokafka-shaped admin double: a broker with a topic catalogue.

    Two real behaviours are modelled on purpose, because a more convenient fake
    would hide the bugs they cause. ``create_topics`` **returns** per-topic error
    codes instead of raising — a fake that raised would let production code with
    a never-firing ``except`` clause pass this suite. And ``describe_topics``
    answers for an absent topic with error code 3 rather than raising or
    omitting it.
    """

    def __init__(
        self,
        *,
        brokers: int = 1,
        catalogue: dict[str, tuple[int, int]] | None = None,  # name -> (partitions, replication)
        start_error: BaseException | None = None,
        force_code: dict[str, tuple[int, str | None]] | None = None,
        v0_response: bool = False,
        **kwargs: object,
    ) -> None:
        self.init_kwargs = kwargs
        self.brokers = brokers
        self.catalogue = dict(catalogue or {})
        self.start_error = start_error
        self.force_code = force_code or {}
        self.v0_response = v0_response
        self.created: list[str] = []  # every topic a create request actually asked for
        self.started = False
        self.closed = False

    async def start(self) -> None:
        if self.start_error is not None:
            raise self.start_error
        self.started = True

    async def close(self) -> None:
        self.closed = True

    async def describe_cluster(self) -> dict[str, Any]:
        return {"brokers": [{"node_id": n} for n in range(self.brokers)]}

    async def describe_topics(self, topics: list[str] | None = None) -> list[dict[str, Any]]:
        names = topics if topics is not None else list(self.catalogue)
        described = []
        for name in names:
            if name in self.catalogue:
                partitions, replication = self.catalogue[name]
                described.append(
                    {
                        "error_code": 0,
                        "topic": name,
                        "partitions": [
                            {"partition": p, "replicas": list(range(replication))}
                            for p in range(partitions)
                        ],
                    }
                )
            else:
                described.append({"error_code": 3, "topic": name, "partitions": []})
        return described

    async def create_topics(
        self, new_topics: list[Any], timeout_ms: int | None = None, validate_only: bool = False
    ) -> FakeCreateResponse:
        errors: list[tuple[Any, ...]] = []
        for new_topic in new_topics:
            name = new_topic.name
            self.created.append(name)
            if name in self.force_code:
                code, message = self.force_code[name]
                if code == 36:
                    # Code 36 means it exists, so a broker that returns it has the
                    # topic — modelling the rival provisioner that just created it.
                    self.catalogue.setdefault(name, (3, 1))
            elif name in self.catalogue:
                code, message = 36, f"Topic '{name}' already exists."
            elif new_topic.replication_factor > self.brokers:
                code, message = (
                    38,
                    (
                        f"Unable to replicate the partition {new_topic.replication_factor} time(s): "
                        f"The target replication factor of {new_topic.replication_factor} cannot be "
                        f"reached because only {self.brokers} broker(s) are registered."
                    ),
                )
            else:
                code, message = 0, None
                self.catalogue[name] = (
                    new_topic.num_partitions,
                    new_topic.replication_factor,
                )
            errors.append((name, code) if self.v0_response else (name, code, message))
        return FakeCreateResponse(errors)


def _specs(partitions: int = 3, replication: int = 1) -> tuple[TopicSpec, ...]:
    return (
        TopicSpec("raw.events.live", partitions, replication),
        TopicSpec("raw.events.dlq", partitions, replication),
    )


async def test_a_fresh_broker_gets_every_required_topic() -> None:
    admin = FakeAdmin()

    report = await ensure_topics(admin, _specs())

    assert admin.catalogue == {"raw.events.live": (3, 1), "raw.events.dlq": (3, 1)}
    assert report.ready
    assert report.as_dict() == {
        "topics": 2,
        "created": 2,
        "existed": 0,
        "missing": 0,
        "drifted": 0,
    }


async def test_provisioning_twice_is_a_no_op_not_an_error() -> None:
    # The headline invariant: provisioning is a deploy step, so it will be re-run
    # by anyone who is unsure whether it ran. That must be free, not a failure.
    admin = FakeAdmin()
    await ensure_topics(admin, _specs())
    creates_after_first = list(admin.created)

    report = await ensure_topics(admin, _specs())

    assert admin.created == creates_after_first  # not one further create was issued
    assert report.ready
    assert report.as_dict()["created"] == 0
    assert report.as_dict()["existed"] == 2


async def test_losing_the_creation_race_is_not_a_failure() -> None:
    # Two operators provisioning at once: describe says absent, and by the time
    # the create lands the other run has won. Both must succeed.
    admin = FakeAdmin(force_code={"raw.events.live": (36, "Topic already exists.")})

    report = await ensure_topics(admin, _specs())

    # Nothing raised, nothing missing: the loser of the race still ends up in the
    # state it wanted, which is the only outcome that makes provisioning safe to
    # run from two places at once.
    assert report.missing == ()
    assert report.ready
    live = next(outcome for outcome in report.outcomes if outcome.name == "raw.events.live")
    assert live.existed and not live.created  # reported honestly as pre-existing


async def test_a_replication_factor_above_the_broker_count_is_refused_before_anything_is_created() -> (
    None
):
    # The zero-side-effect half is what earns this test: discovering the refusal
    # from the response instead would leave the first topic created and the
    # second not, which is a worse state than either doing it or not.
    admin = FakeAdmin(brokers=1)

    with pytest.raises(RuntimeError, match="REPORADAR_KAFKA_TOPIC_REPLICATION_FACTOR"):
        await ensure_topics(admin, _specs(replication=2))

    assert admin.created == []
    assert admin.catalogue == {}


async def test_the_brokers_own_refusal_survives_into_the_error() -> None:
    # The broker's sentence names the real numbers. Replacing it with a
    # tidier message of our own would lose the actionable part.
    admin = FakeAdmin(
        brokers=9, force_code={"raw.events.live": (38, "only 1 broker(s) are registered.")}
    )

    with pytest.raises(RuntimeError, match="only 1 broker"):
        await ensure_topics(admin, _specs(replication=2))


async def test_an_authorization_failure_is_fatal_and_names_the_topic() -> None:
    admin = FakeAdmin(force_code={"raw.events.live": (29, "Not authorized.")})

    with pytest.raises(RuntimeError, match="raw.events.live") as caught:
        await ensure_topics(admin, _specs())

    assert "TopicAuthorizationFailedError" in str(caught.value)


async def test_an_unrecognised_error_code_still_raises_with_its_name() -> None:
    # Guards that the code is mapped through for_code rather than a hand-rolled
    # `if code == 36`, which would swallow every code nobody thought about.
    admin = FakeAdmin(force_code={"raw.events.live": (17, "Invalid topic.")})

    with pytest.raises(RuntimeError, match="InvalidTopicError"):
        await ensure_topics(admin, _specs())


async def test_a_different_partition_count_is_drift_reported_and_never_corrected(
    caplog: pytest.LogCaptureFixture,
) -> None:
    admin = FakeAdmin(catalogue={"raw.events.live": (12, 1), "raw.events.dlq": (3, 1)})

    with caplog.at_level(logging.WARNING, logger="reporadar.ingest.topics"):
        report = await ensure_topics(admin, _specs(partitions=3))

    assert admin.catalogue["raw.events.live"] == (12, 1)  # left exactly as found
    assert admin.created == []
    assert report.ready is False
    assert any("12 partitions" in message and "3" in message for message in caplog.messages)


async def test_drift_is_a_warning_to_ensure_and_a_failure_to_check() -> None:
    # Same broker, two questions. Provisioning must not brick a start on an
    # existing cluster; a deploy gate is allowed to be strict.
    admin = FakeAdmin(catalogue={"raw.events.live": (12, 1), "raw.events.dlq": (3, 1)})

    ensured = await ensure_topics(admin, _specs(partitions=3))
    checked = await verify_topics(admin, _specs(partitions=3))

    assert ensured.as_dict()["drifted"] == 1  # reported, not raised
    assert checked.ready is False


async def test_check_mode_never_creates_anything() -> None:
    admin = FakeAdmin()

    report = await verify_topics(admin, _specs())

    assert admin.created == []
    assert admin.catalogue == {}
    assert report.ready is False
    assert report.missing == ("raw.events.live", "raw.events.dlq")


async def test_a_v0_style_two_tuple_error_is_read_correctly() -> None:
    # Protocol v0 carries no error message. Only an old broker produces this, and
    # an unguarded entry[2] would raise IndexError inside the success path.
    admin = FakeAdmin(v0_response=True)

    report = await ensure_topics(admin, _specs())

    assert report.ready


async def test_both_topics_are_provisioned_with_the_same_partition_count() -> None:
    # The dead-letter sink keys records with the original repo id so a repository's
    # poison messages share the partition its live events use. That promise holds
    # only while the two topics are partitioned identically.
    settings = Settings(kafka_topic_partitions=5, kafka_topic_replication_factor=1)

    specs = required_topics(settings)

    assert {spec.partitions for spec in specs} == {5}
    assert len(specs) == 2


async def test_the_replication_factor_the_guard_refuses_came_from_settings() -> None:
    # The shared `settings` fixture is the one field exempt from "never pin the
    # shipped default", because every admin double here registers a single broker
    # and a higher factor would fail each test inside this guard rather than on its
    # own subject. So the flow from setting to guard is proven here instead, with a
    # factor no single-broker cluster can satisfy: were `required_topics` to ignore
    # the setting and use the default 1, nothing would be raised at all.
    settings = Settings(kafka_topic_replication_factor=2)
    admin = FakeAdmin(brokers=1)

    with pytest.raises(RuntimeError) as caught:
        await ensure_topics(admin, required_topics(settings))

    assert "replication factor 2" in str(caught.value)
    assert "REPORADAR_KAFKA_TOPIC_REPLICATION_FACTOR" in str(caught.value)  # names the fix
    assert admin.created == []  # refused before creating either topic, not after one


async def test_verify_refuses_to_start_and_says_how_to_fix_it(
    monkeypatch: pytest.MonkeyPatch, settings: Settings
) -> None:
    # This single message is the whole point of the story: it replaces a 40-second
    # stall ending in an exception that names neither the topic nor the broker.
    admin = FakeAdmin()
    monkeypatch.setattr(topics, "AIOKafkaAdminClient", lambda **kwargs: admin)

    with pytest.raises(RuntimeError) as caught:
        await require_topics(settings, [settings.kafka_live_topic, settings.kafka_dlq_topic])

    assert settings.kafka_live_topic in str(caught.value)
    assert "reporadar provision" in str(caught.value)
    assert admin.created == []  # the preflight is read-only even when it fails


async def test_require_checks_only_the_topics_it_is_given(
    monkeypatch: pytest.MonkeyPatch, settings: Settings
) -> None:
    # A producer-only run needs the live topic, not the dead-letter one. If it
    # asked for both, it would refuse to start over a topic it never writes to.
    admin = FakeAdmin(catalogue={settings.kafka_live_topic: (2, 1)})  # dlq deliberately absent
    monkeypatch.setattr(topics, "AIOKafkaAdminClient", lambda **kwargs: admin)

    await require_topics(settings, [settings.kafka_live_topic])  # must not raise

    # And it still catches a genuinely missing one it was asked about.
    with pytest.raises(RuntimeError, match=settings.kafka_dlq_topic):
        await require_topics(settings, [settings.kafka_dlq_topic])


async def test_the_admin_factory_wires_settings_and_closes_the_client(
    monkeypatch: pytest.MonkeyPatch, settings: Settings
) -> None:
    built: dict[str, Any] = {}

    def fake_client(**kwargs: Any) -> FakeAdmin:
        admin = FakeAdmin(
            catalogue={settings.kafka_live_topic: (2, 1), settings.kafka_dlq_topic: (2, 1)},
            **kwargs,
        )
        built["admin"] = admin
        return admin

    monkeypatch.setattr(topics, "AIOKafkaAdminClient", fake_client)

    report = await provision_topics(settings)

    admin = built["admin"]
    assert admin.init_kwargs["bootstrap_servers"] == settings.kafka_bootstrap_servers
    assert admin.init_kwargs["client_id"] == topics.ADMIN_CLIENT_ID
    assert admin.started and admin.closed
    assert report.ready


async def test_the_admin_factory_closes_the_client_when_the_run_crashes(
    monkeypatch: pytest.MonkeyPatch, settings: Settings
) -> None:
    admin = FakeAdmin()
    monkeypatch.setattr(topics, "AIOKafkaAdminClient", lambda **kwargs: admin)

    with pytest.raises(ValueError, match="boom"):
        async with kafka_admin(settings):
            raise ValueError("boom")

    assert admin.closed  # a dying run does not leak the connection


async def test_an_unreachable_broker_fails_with_the_configured_address(
    monkeypatch: pytest.MonkeyPatch, settings: Settings
) -> None:
    # aiokafka's own message names the host but not the setting that chose it.
    admin = FakeAdmin(start_error=ConnectionError("Unable to bootstrap"))
    monkeypatch.setattr(topics, "AIOKafkaAdminClient", lambda **kwargs: admin)

    with pytest.raises(RuntimeError) as caught:
        await provision_topics(settings)

    assert settings.kafka_bootstrap_servers in str(caught.value)
    assert "REPORADAR_KAFKA_BOOTSTRAP_SERVERS" in str(caught.value)


def test_the_admin_introduces_itself_distinctly() -> None:
    from reporadar.ingest import kafka

    identities = {
        kafka.CLIENT_ID,
        kafka.DLQ_CLIENT_ID,
        kafka.CONSUMER_CLIENT_ID,
        topics.ADMIN_CLIENT_ID,
    }

    assert len(identities) == 4  # four traffic sources, four names in broker logs


def test_the_admin_timeout_stays_short_enough_to_fail_a_deploy_fast() -> None:
    # A bound, not a value: the exact number is judgement, but a deploy step that
    # waits as long as the client's 40s default reads to an operator as a hang.
    assert 5_000 <= topics.ADMIN_REQUEST_TIMEOUT_MS <= 30_000


# --------------------------------------------------------------------------- #
# What drift detection refuses to answer
# --------------------------------------------------------------------------- #


def test_drift_is_not_reported_for_a_topic_there_was_nothing_to_compare() -> None:
    """Both arms of the early return, and each guards a different false report.

    A topic this run just CREATED matches the spec by construction — it was built
    from it — so comparing it can only produce noise. A topic whose partition count
    came back unknown is the more dangerous case: `None != spec.partitions` is
    perfectly true, and reporting it would tell an operator their cluster had
    drifted when all that happened is that a describe call came back thin.
    """
    spec = TopicSpec(name="reporadar.events.raw", partitions=6, replication_factor=1)

    just_created = TopicOutcome(spec.name, created=True, existed=False, partitions=6)
    assert just_created.drift_from(spec) is None

    partitions_unknown = TopicOutcome(spec.name, created=False, existed=True, partitions=None)
    assert partitions_unknown.drift_from(spec) is None

    # ...and the guard must still let a real disagreement through, or it is a
    # drift check that can never report drift.
    genuinely_drifted = TopicOutcome(
        spec.name, created=False, existed=True, partitions=3, replication_factor=1
    )
    reported = genuinely_drifted.drift_from(spec)
    assert reported is not None
    assert "3 partitions, configured for 6" in reported
