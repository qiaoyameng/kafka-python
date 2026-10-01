# pylint: skip-file
"""Tests for explicit out-of-order message acknowledgment.

Unit tests cover the AckTracker watermark state machine and its
invalidation hooks on SubscriptionState. End-to-end tests drive a real
KafkaConsumer against the in-memory MockCluster (no Kafka service):
out-of-order completion commits only the contiguous acknowledged
prefix, rebalance invalidates stale handles, and close() commits only
the acknowledged prefix.
"""

import time

import pytest

import kafka.errors as Errors
from kafka.consumer.acknowledgment import AckTracker
from kafka.consumer.group import KafkaConsumer
from kafka.consumer.subscription_state import SubscriptionState
from kafka.net.backend.selector import NetworkSelector
from kafka.protocol.consumer import (
    FetchRequest, FetchResponse,
    OffsetCommitRequest, OffsetCommitResponse,
    OffsetFetchRequest, OffsetFetchResponse,
)
from kafka.protocol.metadata import MetadataResponse
from kafka.record.memory_records import MemoryRecordsBuilder
from kafka.structs import OffsetAndMetadata, TopicPartition

from test.mock_broker import MockCluster


TOPIC = 'ack-topic'
TP = TopicPartition(TOPIC, 0)
NUM_RECORDS = 5


# --------------------------------------------------------------------------- #
# AckTracker unit tests                                                       #
# --------------------------------------------------------------------------- #

def test_out_of_order_completion_commits_only_contiguous_prefix():
    """Completing offset 12 must not skip a still-pending offset 11."""
    tracker = AckTracker()
    h10 = tracker.begin(TP, 10)
    h11 = tracker.begin(TP, 11)
    h12 = tracker.begin(TP, 12)

    h12.ack()
    # 12 done, but 10 and 11 pending: nothing committable yet.
    assert tracker.acknowledged_offsets() == {}

    h10.ack()
    # 10 done, 11 still pending: watermark stops at 11; 12 is not skipped.
    assert tracker.acknowledged_offsets() == {TP: OffsetAndMetadata(11, '')}

    h11.ack()
    # Gap closed: watermark advances over the contiguous run 10-12.
    assert tracker.acknowledged_offsets() == {TP: OffsetAndMetadata(13, '')}


def test_fail_leaves_gap_abandon_fills_it():
    """Failed messages keep a hole; only explicit abandon skips them."""
    tracker = AckTracker()
    h0 = tracker.begin(TP, 0)
    h1 = tracker.begin(TP, 1)
    h2 = tracker.begin(TP, 2)

    h0.ack()
    h1.fail()
    h2.ack()
    # 1 failed -> gap; 2 done but not committable past the hole.
    assert tracker.acknowledged_offsets() == {TP: OffsetAndMetadata(1, '')}

    # Explicitly giving up on the failed record releases the watermark.
    h1.abandon()
    assert tracker.acknowledged_offsets() == {TP: OffsetAndMetadata(3, '')}


def test_failed_handle_may_not_be_acked():
    tracker = AckTracker()
    handle = tracker.begin(TP, 0)
    handle.fail()
    with pytest.raises(Errors.IllegalStateError):
        handle.ack()


def test_double_resolve_raises():
    tracker = AckTracker()
    handle = tracker.begin(TP, 0)
    handle.ack()
    assert not handle.valid
    with pytest.raises(Errors.IllegalStateError):
        handle.ack()
    with pytest.raises(Errors.IllegalStateError):
        handle.fail()


def test_invalidate_makes_handles_stale():
    tracker = AckTracker()
    handle = tracker.begin(TP, 5)
    assert handle.valid
    tracker.invalidate(TP)
    assert not handle.valid
    with pytest.raises(Errors.IllegalStateError):
        handle.ack()
    # A new handle after invalidation works and starts a fresh epoch.
    fresh = tracker.begin(TP, 5)
    fresh.ack()
    assert tracker.acknowledged_offsets() == {TP: OffsetAndMetadata(6, '')}


def test_tracker_not_active_until_first_handle():
    tracker = AckTracker()
    assert not tracker.active
    tracker.begin(TP, 0)
    assert tracker.active


# --------------------------------------------------------------------------- #
# SubscriptionState invalidation hooks                                        #
# --------------------------------------------------------------------------- #

@pytest.fixture
def tracked_subscription():
    subscription = SubscriptionState()
    tracker = AckTracker()
    subscription.listeners.append(tracker)
    return subscription, tracker


def test_seek_invalidates_handles(tracked_subscription):
    subscription, tracker = tracked_subscription
    subscription.assign_from_user([TP])
    handle = tracker.begin(TP, 7)
    subscription.seek(TP, 100)
    assert not handle.valid
    with pytest.raises(Errors.IllegalStateError):
        handle.ack()


def test_reassignment_drop_invalidates_handles(tracked_subscription):
    subscription, tracker = tracked_subscription
    subscription.assign_from_user([TP, TopicPartition(TOPIC, 1)])
    handle = tracker.begin(TP, 7)
    kept = tracker.begin(TopicPartition(TOPIC, 1), 3)
    subscription.assign_from_user([TopicPartition(TOPIC, 1)])
    assert not handle.valid
    assert kept.valid


def test_unsubscribe_invalidates_handles(tracked_subscription):
    subscription, tracker = tracked_subscription
    subscription.assign_from_user([TP])
    handle = tracker.begin(TP, 7)
    subscription.unsubscribe()
    assert not handle.valid


# --------------------------------------------------------------------------- #
# End-to-end with MockCluster (no Kafka service)                              #
# --------------------------------------------------------------------------- #

_MetaTopic = MetadataResponse.MetadataResponseTopic
_MetaPartition = _MetaTopic.MetadataResponsePartition
_FetchTopic = FetchResponse.FetchableTopicResponse
_FetchPartition = _FetchTopic.PartitionData
_OffsetCommitTopic = OffsetCommitResponse.OffsetCommitResponseTopic
_OffsetCommitPartition = _OffsetCommitTopic.OffsetCommitResponsePartition
_OffsetFetchTopic = OffsetFetchResponse.OffsetFetchResponseTopic
_OffsetFetchPartition = _OffsetFetchTopic.OffsetFetchResponsePartition
_OffsetFetchGroup = OffsetFetchResponse.OffsetFetchResponseGroup
_OffsetFetchGroupTopic = _OffsetFetchGroup.OffsetFetchResponseTopics
_OffsetFetchGroupPartition = _OffsetFetchGroupTopic.OffsetFetchResponsePartitions


def _record_batch(count=NUM_RECORDS, offset=0):
    builder = MemoryRecordsBuilder(
        magic=2, compression_type=0, batch_size=9999999, offset=offset)
    for i in range(count):
        builder.append(key=None, value=b'msg-%d' % (offset + i),
                       timestamp=0, headers=[])
    builder.close()
    return bytes(builder.buffer())


@pytest.fixture
def cluster():
    mock_cluster = MockCluster(num_brokers=1)
    mock_cluster.set_metadata(topics=[_MetaTopic(
        error_code=0, name=TOPIC, is_internal=False,
        partitions=[_MetaPartition(
            error_code=0, partition_index=0, leader_id=0, leader_epoch=0,
            replica_nodes=[0], isr_nodes=[0], offline_replicas=[])],
    )])
    group = mock_cluster.add_group('ack-group', coordinator=0)
    broker = mock_cluster[0]

    # Committed offset 0 so the consumer starts from the beginning
    # without needing a ListOffsets reset.
    def offset_fetch_handler(api_key, api_version, correlation_id, request_bytes):
        return OffsetFetchResponse(
            throttle_time_ms=0, error_code=0,
            topics=[_OffsetFetchTopic(name=TOPIC, partitions=[
                _OffsetFetchPartition(
                    partition_index=0, committed_offset=0,
                    committed_leader_epoch=-1, metadata='', error_code=0)])],
            groups=[_OffsetFetchGroup(group_id='ack-group', error_code=0, topics=[
                _OffsetFetchGroupTopic(name=TOPIC, partitions=[
                    _OffsetFetchGroupPartition(
                        partition_index=0, committed_offset=0,
                        committed_leader_epoch=-1, metadata='', error_code=0)])])])
    broker.respond_always(OffsetFetchRequest, offset_fetch_handler)

    # Serve the record batch once (at fetch offset 0); empty afterwards.
    def fetch_handler(api_key, api_version, correlation_id, request_bytes):
        request = FetchRequest.decode(
            request_bytes, version=api_version, header=True)
        fetch_offset = request.topics[0].partitions[0].fetch_offset
        records = _record_batch() if fetch_offset == 0 else b''
        return FetchResponse(
            throttle_time_ms=0, error_code=0, session_id=0,
            responses=[_FetchTopic(topic=TOPIC, partitions=[_FetchPartition(
                partition_index=0, error_code=0, high_watermark=NUM_RECORDS,
                last_stable_offset=NUM_RECORDS, log_start_offset=0,
                aborted_transactions=[], preferred_read_replica=-1,
                records=records)])])
    broker.respond_always(FetchRequest, fetch_handler)

    # Capture every committed offset.
    committed = []

    def offset_commit_handler(api_key, api_version, correlation_id, request_bytes):
        request = OffsetCommitRequest.decode(
            request_bytes, version=api_version, header=True)
        for topic in request.topics:
            for partition in topic.partitions:
                committed.append(partition.committed_offset)
        return OffsetCommitResponse(
            throttle_time_ms=0,
            topics=[_OffsetCommitTopic(name=TOPIC, partitions=[
                _OffsetCommitPartition(partition_index=0, error_code=0)])])
    broker.respond_always(OffsetCommitRequest, offset_commit_handler)

    mock_cluster.committed_offsets = committed
    mock_cluster.group = group
    return mock_cluster


@pytest.fixture
def consumer(cluster, monkeypatch):
    # Drive the net loop inline on the caller thread instead of a
    # background IO thread: the started-loop path is not usable in every
    # environment, and inline driving keeps the test deterministic --
    # heartbeats and rebalance progress happen inside poll().
    monkeypatch.setattr(NetworkSelector, 'start', lambda self: None)
    consumer = KafkaConsumer(
        bootstrap_servers=cluster.bootstrap_servers(),
        kafka_client=cluster.client_factory(),
        group_id='ack-group',
        enable_auto_commit=False,
        auto_offset_reset='earliest',
        heartbeat_interval_ms=50,
        request_timeout_ms=5000,
        default_api_timeout_ms=10000,
    )
    consumer.subscribe([TOPIC])
    try:
        yield consumer
    finally:
        consumer.close(autocommit=False)


def _poll_records(consumer, count, timeout=15):
    records = []
    deadline = time.monotonic() + timeout
    while len(records) < count and time.monotonic() < deadline:
        batch = consumer.poll(timeout_ms=500)
        records.extend(batch.get(TP, []))
    assert len(records) >= count, 'timed out waiting for records'
    return records[:count]


def _wait_for(predicate, consumer=None, timeout=15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        if consumer is not None:
            consumer.poll(timeout_ms=100)
        else:
            time.sleep(0.05)
    raise AssertionError('condition not met within timeout')


def test_out_of_order_completion_commit_watermark(cluster, consumer):
    """Out-of-order acks commit only the contiguous completed prefix."""
    records = _poll_records(consumer, NUM_RECORDS)
    handles = {record.offset: consumer.acknowledge(record)
               for record in records}

    # Complete out of order: 2, 4, 0, 1 -- 3 still processing.
    handles[2].ack()
    handles[4].ack()
    handles[0].ack()
    handles[1].ack()

    consumer.commit()
    # 3 is still pending: offset 4's completion must not skip it.
    assert cluster.committed_offsets == [3]

    handles[3].ack()
    consumer.commit()
    assert cluster.committed_offsets == [3, 5]


def test_rebalance_invalidates_stale_handles(cluster, consumer):
    """After a rebalance, pre-rebalance ack handles are dead."""
    records = _poll_records(consumer, NUM_RECORDS)
    handles = [consumer.acknowledge(record) for record in records]
    assert all(handle.valid for handle in handles)

    # Broker-initiated rebalance: next heartbeat gets REBALANCE_IN_PROGRESS.
    cluster.group.trigger_rebalance()
    _wait_for(lambda: not any(handle.valid for handle in handles),
              consumer=consumer)

    with pytest.raises(Errors.IllegalStateError):
        handles[0].ack()

    # The consumer rejoined and is assigned the partition again.
    _wait_for(lambda: consumer.assignment() == {TP}, consumer=consumer)
    # New handles taken after the rebalance work normally (the position
    # is preserved across the rebalance, so no records are re-delivered;
    # the business re-registers the records it was working on).
    fresh = consumer.acknowledge(records[0])
    fresh.ack()
    consumer.commit()
    assert cluster.committed_offsets[-1] == 1


def test_close_commits_only_acknowledged_prefix(cluster, consumer):
    """close() commits the acknowledged prefix, not the fetch position."""
    records = _poll_records(consumer, NUM_RECORDS)
    handles = {record.offset: consumer.acknowledge(record)
               for record in records}

    # 0, 1 done; 2 still in flight; 3 done out of order; 4 failed.
    handles[0].ack()
    handles[1].ack()
    handles[3].ack()
    handles[4].fail()

    consumer.close()

    # Only the contiguous acknowledged prefix {0, 1} is committed (the
    # watermark is offset 2); in-flight offset 2 and everything after
    # it is left uncommitted -- the consumed position (5) is never
    # committed on close.
    assert cluster.committed_offsets == [2]
