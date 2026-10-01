# pylint: skip-file
"""Wire-level tests for the explicit-ack manager using the in-memory
MockBroker.

These drive a real ConsumerCoordinator through an actual rebalance
prepare/complete cycle and a real OffsetCommitRequest over the mock wire,
so the ordering guarantees are verified against the genuine client stack
-- no Kafka broker process required.
"""

import pytest

from kafka.consumer.ack import (
    AckManager, AsyncAckRebalanceListener, StaleAckHandleError,
)
from kafka.consumer.subscription_state import SubscriptionState
from kafka.coordinator.base import Generation, MemberState
from kafka.coordinator.consumer import ConsumerCoordinator
from kafka.protocol.consumer import (
    OffsetCommitRequest, OffsetCommitResponse,
)
from kafka.structs import TopicPartition

TOPIC = 'foobar'
TP0 = TopicPartition(TOPIC, 0)
TP1 = TopicPartition(TOPIC, 1)


class _Record:
    def __init__(self, offset, tp=TP0):
        self.topic = tp.topic
        self.partition = tp.partition
        self.offset = offset


@pytest.fixture
def setup(broker, client, metrics):
    """Bootstrapped coordinator with two assigned partitions and an
    OffsetCommitResponse scripted on the mock wire."""
    committed = []

    def _commit_handler(api_key, api_version, correlation_id, request_bytes):
        req = OffsetCommitRequest.decode(
            request_bytes, version=api_version, header=True)
        for topic in req.topics:
            for part in topic.partitions:
                committed.append((topic.name, part.partition_index,
                                  int(part.committed_offset)))
        _T = OffsetCommitResponse.OffsetCommitResponseTopic
        _P = _T.OffsetCommitResponsePartition
        return OffsetCommitResponse(
            throttle_time_ms=0,
            topics=[_T(name=topic.name, partitions=[
                _P(partition_index=p.partition_index, error_code=0)
                for p in topic.partitions])
                    for topic in req.topics])

    broker.respond_fn(OffsetCommitRequest, _commit_handler)

    coord = ConsumerCoordinator(
        client, SubscriptionState(), metrics=metrics,
        api_version=broker.broker_version,
        enable_auto_commit=False, group_id='ack-group')
    client._manager.bootstrap(timeout_ms=5000)
    coord._subscription.subscribe(topics=[TOPIC])
    coord.coordinator_id = 0
    coord._generation = Generation(7, 'member-1', 'range')
    coord.state = MemberState.STABLE
    coord.rejoin_needed = False
    coord._subscription.assign_from_subscribed([TP0, TP1])
    yield coord, committed
    coord.reset_generation()
    coord.close(timeout_ms=0)


def _revoke(coord, revoked):
    """Drive the genuine EAGER rebalance prepare path on the selector."""
    coord._net.run(coord._on_join_prepare_async, 7, 'member-1')


def _assign(coord, assigned):
    """Drive the genuine join-complete assignment path."""
    from kafka.coordinator.assignors.abstract import ConsumerProtocolAssignment
    assignment = ConsumerProtocolAssignment(
        0, [(TOPIC, sorted({p.partition for p in assigned}))], b'')
    coord._net.run(coord._on_join_complete_async, 8, 'member-1', 'range',
                   assignment.encode())


# --------------------------------------------------------------------------- #
# Out-of-order completion through a real rebalance + wire commit
# --------------------------------------------------------------------------- #

def test_rebalance_commits_only_contiguous_prefix(broker, setup):
    coord, committed = setup
    manager = AckManager(consumer=_ConsumerShim(coord))
    coord._subscription.rebalance_listener = AsyncAckRebalanceListener(manager)

    # Offsets 10/11/12 delivered; workers finish out of order: 12 then 10.
    h10 = manager.track(_Record(10))
    h11 = manager.track(_Record(11))
    h12 = manager.track(_Record(12))
    h12.ack()
    h10.ack()
    # 11 is still in flight when the rebalance starts.
    assert manager.completed_offset(TP0) == 11

    _revoke(coord, {TP0, TP1})

    # The wire received a commit of 11 (next after the contiguous prefix),
    # never 13 which would skip the in-flight 11.
    assert ('foobar', 0, 11) in committed
    assert all(off != 13 for _, _, off in committed)

    # Handles are stale after revocation; late worker results are rejected.
    assert not h11.valid and not h12.valid
    with pytest.raises(StaleAckHandleError):
        h11.ack()


def test_rebalance_with_hole_at_head_commits_nothing(broker, setup):
    coord, committed = setup
    manager = AckManager(consumer=_ConsumerShim(coord))
    coord._subscription.rebalance_listener = AsyncAckRebalanceListener(manager)

    h10 = manager.track(_Record(10))
    h11 = manager.track(_Record(11))
    h11.ack()  # head record 10 still in flight
    assert manager.completed_offset(TP0) == 10

    _revoke(coord, {TP0, TP1})

    # No prefix confirmed -> nothing committed for partition 0.
    assert all(not (name == 'foobar' and part == 0)
               for name, part, _ in committed)
    with pytest.raises(StaleAckHandleError):
        h10.ack()


def test_full_rebalance_cycle_old_handles_dead_new_generation_works(
        broker, setup):
    coord, committed = setup
    manager = AckManager(consumer=_ConsumerShim(coord))
    coord._subscription.rebalance_listener = AsyncAckRebalanceListener(manager)

    old = [manager.track(_Record(i)) for i in (0, 1, 2)]
    old[0].ack()
    old[1].ack()
    # 2 pending through the revoke.
    _revoke(coord, {TP0, TP1})
    assert ('foobar', 0, 2) in committed

    # Reassignment: same partition comes back to us.
    _assign(coord, {TP0, TP1})
    for handle in old:
        assert not handle.valid
        with pytest.raises(StaleAckHandleError):
            handle.ack()

    # New generation tracks fresh offsets (e.g. redelivery from 2).
    new = [manager.track(_Record(i)) for i in (2, 3)]
    new[0].ack()
    new[1].ack()
    assert manager.completed_offset(TP0) == 4


def test_explicit_abandon_fills_hole_before_rebalance(broker, setup):
    coord, committed = setup
    manager = AckManager(consumer=_ConsumerShim(coord))
    coord._subscription.rebalance_listener = AsyncAckRebalanceListener(manager)

    abandoned = []
    manager._on_abandon = lambda tp, off, h: abandoned.append(off)
    handles = [manager.track(_Record(i)) for i in (5, 6, 7)]
    handles[0].ack()
    handles[2].ack()
    # Record 6 failed processing: give it up explicitly so the prefix moves.
    handles[1].abandon()
    assert abandoned == [6]
    assert manager.completed_offset(TP0) == 8

    _revoke(coord, {TP0, TP1})
    assert ('foobar', 0, 8) in committed


def test_force_lost_invalidates_without_commit(broker, setup):
    coord, committed = setup
    manager = AckManager(consumer=_ConsumerShim(coord))
    listener = AsyncAckRebalanceListener(manager)
    coord._subscription.rebalance_listener = listener

    handles = [manager.track(_Record(i)) for i in (0, 1)]
    handles[0].ack()

    coord._net.run(listener.on_partitions_lost, {TP0, TP1})

    assert committed == []  # cannot commit a lost generation
    for handle in handles:
        assert not handle.valid
        with pytest.raises(StaleAckHandleError):
            handle.ack()


class _ConsumerShim:
    """Just enough of KafkaConsumer for AckManager's async commit path."""

    def __init__(self, coordinator):
        self._coordinator = coordinator

    def commit(self, offsets, timeout_ms=None):
        self._coordinator.commit_offsets_sync(offsets, timeout_ms=timeout_ms)
