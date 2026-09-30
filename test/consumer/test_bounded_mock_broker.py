# pylint: skip-file
"""End-to-end bounded-consumption tests against a growing-log MockBroker.

The MockBroker keeps appending messages while the bounded run is in
progress; the run must still stop exactly at the end offsets captured in
its initial snapshot. read_committed snapshots must use the last stable
offset (LSO); log truncation and disappearing partitions must surface as
explicit errors rather than being inferred from an idle timeout.
"""
import pytest

import kafka.errors as Errors
from kafka.consumer.fetcher import Fetcher
from kafka.consumer.subscription_state import SubscriptionState
from kafka.protocol.consumer import (
    FetchRequest, FetchResponse, ListOffsetsRequest, ListOffsetsResponse,
)
from kafka.protocol.metadata import MetadataResponse
from kafka.record.memory_records import MemoryRecordsBuilder
from kafka.structs import OffsetAndMetadata, TopicPartition


TOPIC = 'foobar'
PARTITION = 0

_MetaTopic = MetadataResponse.MetadataResponseTopic
_MetaPartition = _MetaTopic.MetadataResponsePartition
_MetaBroker = MetadataResponse.MetadataResponseBroker
_ListTopic = ListOffsetsResponse.ListOffsetsTopicResponse
_ListPartition = _ListTopic.ListOffsetsPartitionResponse
_FetchTopic = FetchResponse.FetchableTopicResponse
_FetchPartition = _FetchTopic.PartitionData


def _broker_metadata(broker, leader_epoch=3, num_partitions=1):
    broker.set_metadata(
        brokers=[_MetaBroker(node_id=broker.node_id, host=broker.host,
                             port=broker.port, rack=None)],
        topics=[_MetaTopic(
            error_code=0, name=TOPIC, is_internal=False,
            partitions=[_MetaPartition(
                error_code=0, partition_index=i,
                leader_id=broker.node_id, leader_epoch=leader_epoch,
                replica_nodes=[broker.node_id], isr_nodes=[broker.node_id],
                offline_replicas=[]) for i in range(num_partitions)],
        )],
    )


def _build_records(start, count):
    builder = MemoryRecordsBuilder(
        magic=2, compression_type=0, batch_size=9999999, offset=start)
    for i in range(count):
        builder.append(key=None, value=b'msg-%d' % (start + i),
                       timestamp=0, headers=[])
    builder.close()
    return builder.buffer()


def _list_offsets_response(end_offset):
    return ListOffsetsResponse(
        throttle_time_ms=0,
        topics=[_ListTopic(name=TOPIC, partitions=[_ListPartition(
            partition_index=PARTITION, error_code=0,
            old_style_offsets=[], timestamp=-1,
            offset=end_offset, leader_epoch=3)])])


def _fetch_response(records, high_watermark=1000, last_stable_offset=-1):
    return FetchResponse(
        throttle_time_ms=0, error_code=0, session_id=0,
        responses=[_FetchTopic(topic=TOPIC, partitions=[_FetchPartition(
            partition_index=PARTITION, error_code=0,
            high_watermark=high_watermark,
            last_stable_offset=last_stable_offset,
            log_start_offset=-1, aborted_transactions=[],
            preferred_read_replica=-1, records=records)])])


class GrowingLog:
    """Stateful scripted broker backend: a log that grows on every fetch.

    ``initial_size`` messages exist when the bounded snapshot is taken; each
    FetchResponse additionally appends ``grow_by`` messages to the log before
    answering, simulating a producer that keeps writing during the run.
    """

    def __init__(self, initial_size, grow_by=0, initial_lso=None):
        self.log_end = initial_size
        self.lso = initial_lso if initial_lso is not None else initial_size
        self.grow_by = grow_by
        self.fetch_count = 0

    def list_offsets_handler(self, isolation_level=None):
        def _handler(api_key, api_version, correlation_id, request_bytes):
            if isolation_level is not None:
                decoded = ListOffsetsRequest.decode(
                    request_bytes, version=api_version, header=True)
                assert decoded.isolation_level == isolation_level
            # read_committed snapshots must read the LSO, never the LEO.
            end = self.lso if isolation_level == 1 else self.log_end
            return _list_offsets_response(end)
        return _handler

    def fetch_handler(self, api_key, api_version, correlation_id, request_bytes):
        decoded = FetchRequest.decode(
            request_bytes, version=api_version, header=True)
        fetch_offset = decoded.topics[0].partitions[0].fetch_offset
        self.fetch_count += 1
        # A producer appends more messages *before* answering each fetch.
        self.log_end += self.grow_by
        self.lso += self.grow_by
        available = max(0, self.log_end - fetch_offset)
        records = _build_records(fetch_offset, available) if available else b''
        return _fetch_response(records, high_watermark=self.log_end,
                               last_stable_offset=self.lso)


@pytest.fixture
def fetcher_factory(broker, client, manager, metrics):
    _broker_metadata(broker)
    manager.bootstrap(timeout_ms=5000)

    def _make(isolation_level='read_uncommitted'):
        subscription_state = SubscriptionState()
        subscription_state.subscribe(topics=[TOPIC])
        subscription_state.assign_from_subscribed(
            [TopicPartition(TOPIC, PARTITION)])
        subscription_state.seek(TopicPartition(TOPIC, PARTITION), 0)
        return Fetcher(client, subscription_state, metrics=metrics,
                       retry_backoff_ms=20, isolation_level=isolation_level)
    return _make


def _drain_bounded(fetcher, timeout_ms=5000):
    records = []
    while True:
        finished = fetcher.bounded_partitions_finished()
        if finished == set(fetcher._subscriptions.assigned_partitions()):
            break
        batch, _idle = fetcher.fetch_records(timeout_ms=2000)
        for part_records in batch.values():
            records.extend(part_records)
        if not batch and fetcher.bounded_partitions_finished():
            break
    return records


class TestBoundedConsumptionStopsAtSnapshot:
    def test_snapshot_boundary_ignores_messages_appended_after_start(
            self, broker, fetcher_factory):
        """Broker appends a message on every fetch; the run still ends at the
        initial 10-record snapshot and never reads the later messages."""
        tp = TopicPartition(TOPIC, PARTITION)
        log = GrowingLog(initial_size=10, grow_by=1)
        broker.respond_fn(ListOffsetsRequest, log.list_offsets_handler())
        broker.respond_fn(FetchRequest, log.fetch_handler)

        fetcher = fetcher_factory()
        snapshot = fetcher.bounded_end_offsets([tp], timeout_ms=5000)
        assert snapshot == {tp: 10}
        fetcher.begin_bounded_consumption(snapshot)

        records = _drain_bounded(fetcher)

        assert [r.offset for r in records] == list(range(10))
        assert log.fetch_count >= 1
        # The broker kept growing beyond the snapshot ...
        assert log.log_end > 10
        # ... but no further fetch is issued once position == end offset.
        fetches_after_boundary = log.fetch_count
        fetcher.send_fetches()
        assert log.fetch_count == fetches_after_boundary
        assert fetcher.bounded_remaining(tp) == 0
        fetcher.end_bounded_consumption()

    def test_records_past_boundary_in_a_batch_are_clipped(
            self, broker, fetcher_factory):
        """One fetch response containing records beyond the boundary must be
        trimmed so position lands exactly on the snapshot end offset."""
        tp = TopicPartition(TOPIC, PARTITION)
        log = GrowingLog(initial_size=3, grow_by=0)
        broker.respond_fn(ListOffsetsRequest, log.list_offsets_handler())

        def big_fetch(api_key, api_version, correlation_id, request_bytes):
            # Return 10 records although snapshot end is only 3.
            return _fetch_response(_build_records(0, 10), high_watermark=10)
        broker.respond_fn(FetchRequest, big_fetch)

        fetcher = fetcher_factory()
        snapshot = fetcher.bounded_end_offsets([tp], timeout_ms=5000)
        fetcher.begin_bounded_consumption(snapshot)

        records, _ = fetcher.fetch_records(timeout_ms=2000)
        offsets = [r.offset for records_tp in records.values() for r in records_tp]
        assert offsets == [0, 1, 2]
        assert fetcher._subscriptions.position(tp).offset == 3
        assert fetcher.bounded_partitions_finished() == {tp}
        fetcher.end_bounded_consumption()

    def test_read_committed_snapshot_uses_last_stable_offset(
            self, broker, fetcher_factory):
        """With read_committed, the ListOffsets request carries isolation
        level READ_COMMITTED and the snapshot boundary is the LSO, even while
        the log end offset (open transaction) is further ahead."""
        tp = TopicPartition(TOPIC, PARTITION)
        # 10 messages total, but only 8 are part of committed transactions.
        log = GrowingLog(initial_size=10, grow_by=2, initial_lso=8)
        broker.respond_fn(
            ListOffsetsRequest, log.list_offsets_handler(isolation_level=1))
        broker.respond_fn(FetchRequest, log.fetch_handler)

        fetcher = fetcher_factory(isolation_level='read_committed')
        snapshot = fetcher.bounded_end_offsets([tp], timeout_ms=5000)
        assert snapshot == {tp: 8}
        fetcher.begin_bounded_consumption(snapshot)

        records = _drain_bounded(fetcher)
        assert [r.offset for r in records] == list(range(8))
        assert log.log_end > 10  # broker kept appending
        fetcher.end_bounded_consumption()

    def test_no_fetch_when_snapshot_starts_empty(self, broker, fetcher_factory):
        """Empty partitions at snapshot time complete immediately even though
        messages arrive immediately afterwards - they are outside the run."""
        tp = TopicPartition(TOPIC, PARTITION)
        log = GrowingLog(initial_size=0, grow_by=5)
        broker.respond_fn(ListOffsetsRequest, log.list_offsets_handler())
        broker.respond_fn(FetchRequest, log.fetch_handler)

        fetcher = fetcher_factory()
        snapshot = fetcher.bounded_end_offsets([tp], timeout_ms=5000)
        assert snapshot == {tp: 0}
        fetcher.begin_bounded_consumption(snapshot)

        assert fetcher.bounded_partitions_finished() == {tp}
        requests = fetcher._create_fetch_requests()
        assert requests == {}
        fetcher.end_bounded_consumption()

    def test_idle_partition_with_records_remaining_is_not_finished(
            self, broker, fetcher_factory):
        """A quiet broker that returns no records must NOT be treated as
        finished while the position is still below the snapshot boundary:
        completion is position-vs-snapshot, never an idle timeout."""
        tp = TopicPartition(TOPIC, PARTITION)
        log = GrowingLog(initial_size=2, grow_by=0)
        broker.respond_fn(ListOffsetsRequest, log.list_offsets_handler())
        broker.respond_fn(FetchRequest, log.fetch_handler)

        fetcher = fetcher_factory()
        snapshot = fetcher.bounded_end_offsets([tp], timeout_ms=5000)
        fetcher.begin_bounded_consumption(snapshot)

        # No records consumed yet: broker being idle (poll timeouts) must not
        # look like completion even if an external caller sees empty polls.
        assert fetcher.bounded_partitions_finished() == set()
        assert fetcher.bounded_remaining(tp) == 2

        records = _drain_bounded(fetcher)
        assert [r.offset for r in records] == [0, 1]
        assert fetcher.bounded_partitions_finished() == {tp}
        fetcher.end_bounded_consumption()


class TestBoundedConsumptionErrors:
    def test_truncation_raises_instead_of_silent_reset(
            self, broker, fetcher_factory):
        """OffsetOutOfRange at snapshot position must surface explicitly;
        bounded mode never auto-resets past missing records."""
        tp = TopicPartition(TOPIC, PARTITION)
        log = GrowingLog(initial_size=5, grow_by=0)
        broker.respond_fn(ListOffsetsRequest, log.list_offsets_handler())

        def truncating_fetch(api_key, api_version, correlation_id, request_bytes):
            return FetchResponse(
                throttle_time_ms=0, error_code=0, session_id=0,
                responses=[_FetchTopic(topic=TOPIC, partitions=[
                    _FetchPartition(
                        partition_index=PARTITION,
                        error_code=Errors.OffsetOutOfRangeError.errno,
                        high_watermark=-1, last_stable_offset=-1,
                        log_start_offset=-1, aborted_transactions=[],
                        preferred_read_replica=-1, records=b'')])])
        broker.respond_fn(FetchRequest, truncating_fetch)

        fetcher = fetcher_factory()
        snapshot = fetcher.bounded_end_offsets([tp], timeout_ms=5000)
        fetcher.begin_bounded_consumption(snapshot)
        # The subscription's default auto-reset would normally seek to
        # earliest; bounded mode must refuse and raise instead.
        with pytest.raises(Errors.OffsetOutOfRangeError):
            fetcher.fetch_records(timeout_ms=2000)
        fetcher.end_bounded_consumption()

    def test_missing_partition_at_snapshot_raises(self, broker, client, manager, metrics):
        """A partition absent from metadata when the snapshot is taken is
        reported explicitly instead of hanging on ListOffsets retries."""
        tp = TopicPartition(TOPIC, PARTITION)
        # Metadata advertises no topics at all.
        broker.set_metadata(topics=[])
        manager.bootstrap(timeout_ms=5000)
        manager.cluster.request_update()

        subscription_state = SubscriptionState()
        subscription_state.subscribe(topics=[TOPIC])
        subscription_state.assign_from_subscribed([tp])
        subscription_state.seek(tp, 0)
        fetcher = Fetcher(client, subscription_state, metrics=metrics)

        broker.respond_fn(
            ListOffsetsRequest,
            lambda *a: ListOffsetsResponse(
                throttle_time_ms=0,
                topics=[_ListTopic(name=TOPIC, partitions=[_ListPartition(
                    partition_index=PARTITION,
                    error_code=Errors.UnknownTopicOrPartitionError.errno,
                    old_style_offsets=[], timestamp=-1, offset=-1,
                    leader_epoch=-1)])]))

        with pytest.raises(Errors.PartitionUnavailableError):
            fetcher.bounded_end_offsets([tp], timeout_ms=200)

    def test_position_moved_beyond_boundary_is_finished_and_not_refetched(
            self, broker, fetcher_factory):
        """A position at/past the boundary (e.g. after an explicit seek) must
        be treated as finished and never trigger another fetch for newly
        appended data; the high-level consumer rejects such seeks directly."""
        tp = TopicPartition(TOPIC, PARTITION)
        log = GrowingLog(initial_size=3, grow_by=0)
        broker.respond_fn(ListOffsetsRequest, log.list_offsets_handler())

        fetcher = fetcher_factory()
        snapshot = fetcher.bounded_end_offsets([tp], timeout_ms=5000)
        fetcher.begin_bounded_consumption(snapshot)
        try:
            fetcher._subscriptions.assignment[tp].position = OffsetAndMetadata(
                snapshot[tp], '', -1)
            assert fetcher.bounded_partitions_finished() == {tp}
            assert fetcher._fetchable_partitions() == []
            assert fetcher._create_fetch_requests() == {}
        finally:
            fetcher.end_bounded_consumption()
