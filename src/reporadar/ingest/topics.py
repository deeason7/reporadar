"""Kafka topic provisioning: create explicitly, verify automatically.

The broker does not create these topics on demand — auto-creation is off in the
local stack on purpose, and the client's metadata requests predate the field that
would ask for it. So a process pointed at a fresh broker stalls for the whole
request timeout and then raises an exception carrying no topic name and no
address. That silence is the problem this module exists to remove.

Two mechanisms, deliberately separate, because they answer to different
authorities. **Creating** a topic fixes its partition count, and the partition
count is what maps a repository id onto a partition — change it later and every
key moves, breaking the per-repository ordering the wire contract promises. That
is an operational decision, so it belongs to a command a human runs.
**Verifying** that the topics already exist is a startup precondition, so it
belongs in the path that would otherwise fail obscurely.

One quirk shapes everything here: ``create_topics`` does **not** raise. It
returns per-topic error codes and never inspects them, unlike the admin client's
other calls. Code written as ``try: ... except TopicAlreadyExistsError`` — the
shape most examples use — would have an ``except`` clause that never fires and
would read every failure as success.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, Protocol

from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import for_code
from pydantic import BaseModel, ConfigDict

from reporadar.config import Settings

logger = logging.getLogger(__name__)

ADMIN_CLIENT_ID = "reporadar-admin"  # the fourth distinct identity in broker logs

# Provisioning is a deploy step, not a loop: a broker that cannot answer in this
# window is a broken deploy, and the client's 40s default is long enough that an
# operator reads it as a hang. Short enough to fail a deploy fast, long enough to
# outlast a controller election.
ADMIN_REQUEST_TIMEOUT_MS = 10_000

_NO_ERROR = 0
_TOPIC_ALREADY_EXISTS = 36
_UNKNOWN_TOPIC = 3


@dataclass(frozen=True)
class TopicSpec:
    """One topic as this pipeline needs it to exist."""

    name: str
    partitions: int
    replication_factor: int


def required_topics(settings: Settings) -> tuple[TopicSpec, ...]:
    """The topics the pipeline needs, both sized identically — see ``config``."""
    return (
        TopicSpec(
            settings.kafka_live_topic,
            settings.kafka_topic_partitions,
            settings.kafka_topic_replication_factor,
        ),
        TopicSpec(
            settings.kafka_dlq_topic,
            settings.kafka_topic_partitions,
            settings.kafka_topic_replication_factor,
        ),
    )


class PartitionState(BaseModel):
    """One partition's replica assignment, as the broker reports it."""

    model_config = ConfigDict(extra="ignore")

    replicas: list[int]


class TopicState(BaseModel):
    """One topic exactly as ``describe_topics`` reports it.

    Extra keys are tolerated because the metadata payload grows fields across
    protocol versions (``is_internal``, ``offline_replicas``) and none of them
    change what provisioning decides.
    """

    model_config = ConfigDict(extra="ignore")

    error_code: int
    topic: str
    partitions: list[PartitionState] = []

    @property
    def exists(self) -> bool:
        return self.error_code == _NO_ERROR

    @property
    def partition_count(self) -> int:
        return len(self.partitions)

    @property
    def replication_factor(self) -> int:
        # Uniform across partitions in every configuration this provisions.
        return len(self.partitions[0].replicas) if self.partitions else 0


@dataclass(frozen=True)
class TopicOutcome:
    """What provisioning found or did for one topic."""

    name: str
    created: bool
    existed: bool
    partitions: int | None = None
    replication_factor: int | None = None

    @property
    def missing(self) -> bool:
        return not (self.created or self.existed)

    def drift_from(self, spec: TopicSpec) -> str | None:
        """A sentence describing how the live topic differs from the spec, if it does."""
        if not self.existed or self.partitions is None:
            return None
        differences = []
        if self.partitions != spec.partitions:
            differences.append(f"{self.partitions} partitions, configured for {spec.partitions}")
        if self.replication_factor != spec.replication_factor:
            differences.append(
                f"replication {self.replication_factor}, configured for {spec.replication_factor}"
            )
        return f"{self.name} has {' and '.join(differences)}" if differences else None


@dataclass(frozen=True)
class ProvisionReport:
    """The result of one provisioning or verification pass."""

    outcomes: tuple[TopicOutcome, ...]
    drifts: tuple[str, ...] = ()

    @property
    def ready(self) -> bool:
        """Every topic present and matching its spec — the deploy-gate question."""
        return not self.drifts and all(not outcome.missing for outcome in self.outcomes)

    @property
    def missing(self) -> tuple[str, ...]:
        return tuple(outcome.name for outcome in self.outcomes if outcome.missing)

    def as_dict(self) -> dict[str, int]:
        """The machine-readable seam, matching the counters elsewhere."""
        return {
            "topics": len(self.outcomes),
            "created": sum(outcome.created for outcome in self.outcomes),
            "existed": sum(outcome.existed for outcome in self.outcomes),
            "missing": len(self.missing),
            "drifted": len(self.drifts),
        }


class TopicAdmin(Protocol):
    """The slice of an async Kafka admin client provisioning needs (aiokafka-shaped).

    ``create_topics`` returns per-topic error codes rather than raising; a fake
    that raised instead would let the real quirk through untested.
    """

    async def describe_cluster(self) -> Mapping[str, Any]: ...

    # Sequence, not list: list is invariant, so a list[dict] — which is what both
    # the real client and any fake actually return — would not satisfy a
    # list[Mapping] annotation.
    async def describe_topics(
        self, topics: list[str] | None = None
    ) -> Sequence[Mapping[str, Any]]: ...

    async def create_topics(
        self,
        new_topics: list[Any],  # aiokafka NewTopic; the library ships no type information
        timeout_ms: int | None = None,
        validate_only: bool = False,
    ) -> Any: ...  # a protocol Response; only .topic_errors is read


def _normalise_errors(response: Any) -> list[tuple[str, int, str | None]]:
    """``topic_errors`` entries, arity-normalised.

    Protocol v0 carries ``(topic, code)``; v1+ adds a message. Normalising here
    means one place knows that, rather than every caller.
    """
    normalised: list[tuple[str, int, str | None]] = []
    for entry in response.topic_errors:
        topic, code = entry[0], entry[1]
        message = entry[2] if len(entry) > 2 else None
        normalised.append((topic, code, message))
    return normalised


async def describe(admin: TopicAdmin, names: Sequence[str]) -> dict[str, TopicState]:
    """Current broker state for ``names``. Read-only: it never creates anything."""
    described = await admin.describe_topics(list(names))
    states = [TopicState.model_validate(dict(entry)) for entry in described]
    for state in states:
        if not state.exists and state.error_code != _UNKNOWN_TOPIC:
            raise RuntimeError(
                f"cannot read topic {state.topic!r}: "
                f"{for_code(state.error_code).__name__} (code {state.error_code})"
            )
    return {state.topic: state for state in states}


def _outcome(spec: TopicSpec, state: TopicState) -> TopicOutcome:
    if not state.exists:
        return TopicOutcome(spec.name, created=False, existed=False)
    return TopicOutcome(
        spec.name,
        created=False,
        existed=True,
        partitions=state.partition_count,
        replication_factor=state.replication_factor,
    )


def _report(specs: Sequence[TopicSpec], outcomes: Sequence[TopicOutcome]) -> ProvisionReport:
    drifts = []
    for spec, outcome in zip(specs, outcomes, strict=True):
        if (drift := outcome.drift_from(spec)) is not None:
            # Deliberately a warning and never corrected: a topic sized differently
            # is an operational decision this process must not overrule, and the
            # correction would silently re-map every key. --check is where a deploy
            # gets to be strict about it.
            logger.warning("topic configuration drift: %s (left as found)", drift)
            drifts.append(drift)
    return ProvisionReport(tuple(outcomes), tuple(drifts))


async def verify_topics(admin: TopicAdmin, specs: Sequence[TopicSpec]) -> ProvisionReport:
    """Report what exists, creating nothing."""
    states = await describe(admin, [spec.name for spec in specs])
    return _report(specs, [_outcome(spec, states[spec.name]) for spec in specs])


async def ensure_topics(admin: TopicAdmin, specs: Sequence[TopicSpec]) -> ProvisionReport:
    """Create whichever of ``specs`` are absent. Idempotent: re-running is a no-op."""
    cluster = await admin.describe_cluster()
    brokers = len(cluster.get("brokers", []))
    wanted = max(spec.replication_factor for spec in specs)
    if brokers and wanted > brokers:
        # Checked before anything is created: discovering this from the response
        # can leave the first topic created and the second not.
        raise RuntimeError(
            f"replication factor {wanted} exceeds the {brokers} registered broker(s); "
            f"lower REPORADAR_KAFKA_TOPIC_REPLICATION_FACTOR or add brokers"
        )

    states = await describe(admin, [spec.name for spec in specs])
    absent = [spec for spec in specs if not states[spec.name].exists]
    created: set[str] = set()
    if absent:
        response = await admin.create_topics(
            [NewTopic(s.name, s.partitions, s.replication_factor) for s in absent],
            timeout_ms=ADMIN_REQUEST_TIMEOUT_MS,
        )
        for topic, code, message in _normalise_errors(response):
            if code == _NO_ERROR:
                created.add(topic)
                logger.info("provisioned %s", topic)
            elif code == _TOPIC_ALREADY_EXISTS:
                # Another provisioner won the race. Both runs must succeed.
                logger.info("topic %s already existed", topic)
            else:
                # The broker's own sentence is better than anything written here
                # ("...only 1 broker(s) are registered"), so it is surfaced verbatim.
                raise RuntimeError(
                    f"could not create {topic!r}: {for_code(code).__name__} "
                    f"(code {code}){f': {message}' if message else ''}"
                )

    refreshed = await describe(admin, [spec.name for spec in specs])
    outcomes = []
    for spec in specs:
        state = refreshed[spec.name]
        outcomes.append(
            TopicOutcome(
                spec.name,
                created=spec.name in created,
                existed=state.exists and spec.name not in created,
                partitions=state.partition_count if state.exists else None,
                replication_factor=state.replication_factor if state.exists else None,
            )
        )
    return _report(specs, outcomes)


@asynccontextmanager
async def kafka_admin(settings: Settings) -> AsyncIterator[TopicAdmin]:
    """A started admin client, closed on exit — crash or not.

    Mirrors ``kafka_sink``/``kafka_source``, except this client's teardown is
    ``close()`` rather than ``stop()``; wrapping it is what keeps that asymmetry
    from reaching every caller.
    """
    admin = AIOKafkaAdminClient(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        client_id=ADMIN_CLIENT_ID,
        request_timeout_ms=ADMIN_REQUEST_TIMEOUT_MS,
    )
    try:
        await admin.start()
    except Exception as exc:  # noqa: BLE001 — re-raised immediately, named
        raise RuntimeError(
            f"cannot reach the Kafka broker at {settings.kafka_bootstrap_servers!r} "
            f"(REPORADAR_KAFKA_BOOTSTRAP_SERVERS)"
        ) from exc
    try:
        yield admin
    finally:
        await admin.close()


async def provision_topics(settings: Settings, *, check_only: bool = False) -> ProvisionReport:
    """Create the pipeline's topics, or report on them without touching anything."""
    specs = required_topics(settings)
    async with kafka_admin(settings) as admin:
        if check_only:
            return await verify_topics(admin, specs)
        return await ensure_topics(admin, specs)


async def require_topics(settings: Settings, topics: Sequence[str]) -> None:
    """Fail fast, and legibly, when a topic the caller needs does not exist.

    Takes the topics *this* caller depends on rather than all of them: the
    consumer needs both the live and dead-letter topics, but the producer only
    writes to the live one, and a producer-only deployment should not refuse to
    start over a dead-letter topic it never touches. Without this a client blocks
    for its whole request timeout and then raises an exception naming neither the
    topic nor the broker.
    """
    async with kafka_admin(settings) as admin:
        states = await describe(admin, topics)
    missing = [name for name in topics if not states[name].exists]
    if missing:
        raise RuntimeError(
            f"topic(s) {', '.join(missing)} do not exist on {settings.kafka_bootstrap_servers}; "
            f"run 'reporadar provision' to create them"
        )
