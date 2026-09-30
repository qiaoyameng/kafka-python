# pylint: skip-file
"""End-to-end tests for KafkaConsumer.bounded_consume() (offline reconciliation).

The MockBroker keeps a small in-memory partition log that tests can keep
appending to, so these tests prove bounded iteration stops exactly at the
snapshot taken at startup even while new messages arrive afterwards.
"""

import threading

import pytest

import kafka.errors as Errors
from kafka import KafkaConsumer
from kafka.protocol.consumer import (
    FetchRequest, FetchResponse, ListOffsetsRequest, ListOffsetsResponse,
)
from kafka.protocol.metadata import MetadataResponse
from kafka.record.memory_records import MemoryRecordsBuilder
from kafka.structs import TopicPartition


TOPIC = 'offline'
TP = TopicPartition(TOPIC, 0)

_MetaTopic = MetadataResponse.MetadataResponseTopic
_MetaPartition = _MetaTopic.MetadataResponsePartition
_MetaBroker = MetadataResponse.MetadataResponseBroker
_ListTopic = ListOffsetsResponse.ListOffsetsTopicResponse
_ListPartition = _ListTopic.ListOffsetsPartitionResponse
_FetchTopic = FetchResponse.FetchableTopicResponse
_FetchPartition = _FetchTopic.PartitionData


class _FakeLog:
    """Tiny append-only log. end offset == number of committed records."""

    def __init__(self):
        self._lock = threading.Lock()
        self._records = []  # list of (key, value)

    def append(self, value, key=b'k'):
        with self._lock:
            self._records.append((key, value))

    def extend(self, values):
        for value in values:
            self.append(value)

    def __len__(self):
        with self._lock:
            return len(self._records)

    def records(self, fetch_offset, max_offset, high_watermark):
        with self._lock:
            slice_ = self._records[fetch_offset:max_offset]
        return _record_batch(fetch_offset, slice_, high_watermark=high_watermark)


def _record_batch(offset, kv_pairs, high_watermark=1000000):
    builder = MemoryRecordsBuilder(
        magic=2, compression_type=0, batch_size=9999999, offset=offset)
    now = 0
    for key, value in kv_pairs:
        builder.append(key=key, value=value, timestamp=now, headers=[])
        now += 1
    builder.close()
    return builder.buffer()


def _set_topic_metadata(broker, exists=True):
    partitions = []
    if exists:
        partitions.append(_MetaPartition(version=8,
            error_code=0, partition_index=0, leader_id=broker.node_id,
            leader_epoch=1, replica_nodes=[broker.node_id],
            isr_nodes=[broker.node_id], offline_replicas=[]))
    broker.set_metadata(
        brokers=[_MetaBroker(node_id=broker.node_id, host=broker.host,
                             port=broker.port, rack=None)],
        topics=[_MetaTopic(
            version=8, error_code=0, name=TOPIC, is_internal=False,
            partitions=partitions)])


def _install_handlers(broker, log, *, isolation_level=0, fetch_error=0,
                      deleted_after_snapshot=False):
    """Wire ListOffsets + Fetch against ``log``.

    ``deleted_after_snapshot``: the first ListOffsets (the snapshot)
    succeeds; subsequent metadata/list-offset/fetch calls act like the
    partition was deleted.
    """
    state = {'snapshotted': False}

    def list_offsets_handler(api_key, api_version, correlation_id, request_bytes):
        req = ListOffsetsRequest.decode(request_bytes, version=api_version, header=True)
        assert req.isolation_level == isolation_level
        # Earliest reset (timestamp -2) -> 0; snapshot/latest (-1) -> log end.
        requested_ts = None
        for _topic, parts in req.topics:
            for part in parts:
                requested_ts = part.timestamp
        if deleted_after_snapshot:
            if state['snapshotted']:
                end = -1
                error_code = Errors.UnknownTopicOrPartitionError.errno
            else:
                state['snapshotted'] = True
                end = 0 if requested_ts == -2 else len(log)
                error_code = 0
        else:
            end = 0 if requested_ts == -2 else len(log)
            error_code = 0
        return ListOffsetsResponse(
            throttle_time_ms=0,
            topics=[_ListTopic(name=TOPIC, partitions=[_ListPartition(
                partition_index=0, error_code=error_code,
                timestamp=-1, offset=end, leader_epoch=1)])])

    def fetch_handler(api_key, api_version, correlation_id, request_bytes):
        req = FetchRequest.decode(request_bytes, version=api_version, header=True)
        fetch_offset = None
        for topic, parts in req.topics:
            assert topic == TOPIC
            for part in parts:
                partition = part.partition
                offset = part.fetch_offset
                if partition == 0:
                    fetch_offset = offset
        assert fetch_offset is not None
        if deleted_after_snapshot:
            return FetchResponse(
                throttle_time_ms=0, error_code=0, session_id=0,
                responses=[_FetchTopic(topic=TOPIC, partitions=[_FetchPartition(
                    partition_index=0,
                    error_code=Errors.UnknownTopicOrPartitionError.errno,
                    high_watermark=-1, last_stable_offset=-1,
                    log_start_offset=-1, aborted_transactions=[],
                    preferred_read_replica=-1, records=b'')])])
        end = len(log)
        records = log.records(fetch_offset, end, high_watermark=end) if fetch_offset < end else b''
        return FetchResponse(
            throttle_time_ms=0, error_code=0, session_id=0,
            responses=[_FetchTopic(topic=TOPIC, partitions=[_FetchPartition(
                partition_index=0, error_code=fetch_error,
                high_watermark=end, last_stable_offset=end,
                log_start_offset=0, aborted_transactions=[],
                preferred_read_replica=-1, records=records)])])

    broker.respond_always(ListOffsetsRequest, list_offsets_handler)
    broker.respond_always(FetchRequest, fetch_handler)


def _make_consumer(broker, **kwargs):
    """Build a KafkaConsumer whose internal client is attached to the
    MockBroker at construction time, so bootstrap traffic is mocked too."""
    configs = dict(
        bootstrap_servers='%s:%d' % (broker.host, broker.port),
        api_version=broker.broker_version,
        group_id=None,
        enable_auto_commit=False,
        auto_offset_reset='earliest',
        request_timeout_ms=5000,
        default_api_timeout_ms=10000,
        fetch_max_wait_ms=100,
    )
    configs.update(kwargs)

    consumer = KafkaConsumer(kafka_client=broker.client_factory(), **configs)
    consumer.assign([TP])
    return consumer


@pytest.fixture(autouse=True)
def _inline_net(monkeypatch):
    """Run the event loop inline (no background IO thread).

    The selector's cross-thread wakeup uses a socketpair, which the
    sandbox blocks; the rest of the mock-broker suite drives the loop
    inline for the same reason. Background-thread-only code paths are
    covered by the real-broker integration suite.
    """
    from kafka.net.backend.selector import NetworkSelector
    monkeypatch.setattr(NetworkSelector, 'start', lambda self: None)
    monkeypatch.setattr(NetworkSelector, 'stop', lambda self: None)


@pytest.fixture
def wired_broker(broker):
    _set_topic_metadata(broker, exists=True)
    return broker


class TestBoundedConsume:
    def test_stops_at_initial_snapshot_despite_continuous_appends(self, wired_broker):
        log = _FakeLog()
        log.extend([b'msg-%d' % i for i in range(10)])
        _install_handlers(wired_broker, log)

        consumer = _make_consumer(wired_broker)
        try:
            iterator = consumer.bounded_consume()
            received = []
            # The broker keeps appending "new" messages the whole time;
            # none of them belong to this offline task.
            for _ in range(30):
                try:
                    received.append(next(iterator))
                except StopIteration:
                    break
                log.append(b'AFTER-SNAPSHOT-%d' % len(log))

            # Exactly the ten pre-snapshot records, offsets 0..9.
            assert [r.offset for r in received] == list(range(10))
            assert [r.value for r in received] == [b'msg-%d' % i for i in range(10)]

            # The iterator is exhausted and would not pull later messages.
            with pytest.raises(StopIteration):
                next(iterator)
            # Even though the log grew far beyond the snapshot.
            assert len(log) > 10
            # Position parked exactly on the recorded boundary, paused.
            assert consumer.position(TP) == 10
            assert TP in consumer.paused()
        finally:
            consumer.close()

    def test_skips_no_records_when_position_at_end(self, wired_broker):
        log = _FakeLog()
        _install_handlers(wired_broker, log)
        consumer = _make_consumer(wired_broker)
        try:
            consumer.seek(TP, 0)
            assert list(consumer.bounded_consume()) == []
            assert consumer.position(TP) == 0
            assert TP in consumer.paused()
        finally:
            consumer.close()

    def test_batch_extending_past_boundary_is_clipped(self, broker):
        # Snapshot is fixed at 5 by ListOffsets. Immediately afterwards the
        # broker appends more records, so the first Fetch returns a batch
        # extending beyond the boundary. Bounded iteration must clip at 5.
        _set_topic_metadata(broker, exists=True)
        log = _FakeLog()
        log.extend([b'm-%d' % i for i in range(5)])

        def list_offsets_handler(api_key, api_version, correlation_id, request_bytes):
            req = ListOffsetsRequest.decode(request_bytes, version=api_version, header=True)
            ts = req.topics[0][1][0].timestamp
            end = 0 if ts == -2 else 5
            # Broker keeps appending after the snapshot, before any fetch.
            log.extend([b'LATE-%d' % i for i in range(7)])
            return ListOffsetsResponse(
                throttle_time_ms=0,
                topics=[_ListTopic(name=TOPIC, partitions=[_ListPartition(
                    partition_index=0, error_code=0,
                    timestamp=-1, offset=end, leader_epoch=1)])])

        def fetch_handler(api_key, api_version, correlation_id, request_bytes):
            req = FetchRequest.decode(request_bytes, version=api_version, header=True)
            fetch_offset = req.topics[0][1][0].fetch_offset
            end = len(log)
            records = log.records(fetch_offset, end, high_watermark=end)
            return FetchResponse(
                throttle_time_ms=0, error_code=0, session_id=0,
                responses=[_FetchTopic(topic=TOPIC, partitions=[_FetchPartition(
                    partition_index=0, error_code=0,
                    high_watermark=end, last_stable_offset=end,
                    log_start_offset=0, aborted_transactions=[],
                    preferred_read_replica=-1, records=records)])])

        broker.respond_always(ListOffsetsRequest, list_offsets_handler)
        broker.respond_always(FetchRequest, fetch_handler)
        consumer = _make_consumer(broker)
        try:
            received = list(consumer.bounded_consume())
            assert [r.offset for r in received] == [0, 1, 2, 3, 4]
            assert all(not r.value.startswith(b'LATE') for r in received)
            assert consumer.position(TP) == 5
            assert TP in consumer.paused()
            assert len(log) > 5  # the broker kept growing past the snapshot
        finally:
            consumer.close()

    def test_resume_after_bounded_run_reads_new_messages(self, wired_broker):
        log = _FakeLog()
        log.extend([b'old-%d' % i for i in range(3)])
        _install_handlers(wired_broker, log)
        consumer = _make_consumer(wired_broker)
        try:
            first = list(consumer.bounded_consume())
            assert [r.value for r in first] == [b'old-0', b'old-1', b'old-2']

            log.extend([b'new-%d' % i for i in range(2)])
            consumer.resume(TP)
            polled = consumer.poll(timeout_ms=2000)
            values = [r.value for batch in polled.values() for r in batch]
            assert values == [b'new-0', b'new-1']
        finally:
            consumer.close()

    def test_seek_past_boundary_raises(self, wired_broker):
        log = _FakeLog()
        log.extend([b'm-%d' % i for i in range(5)])
        _install_handlers(wired_broker, log)
        consumer = _make_consumer(wired_broker)
        try:
            consumer.seek(TP, 50)
            with pytest.raises(Errors.OffsetOutOfRangeError):
                list(consumer.bounded_consume())
        finally:
            consumer.close()

    def test_partition_gone_at_snapshot_raises(self, broker):
        _set_topic_metadata(broker, exists=False)
        log = _FakeLog()
        _install_handlers(broker, log, deleted_after_snapshot=True)
        consumer = _make_consumer(broker)
        try:
            with pytest.raises(Errors.SnapshotPartitionNotFoundError):
                list(consumer.bounded_consume())
        finally:
            consumer.close()

    def test_partition_deleted_mid_run_raises(self, wired_broker):
        log = _FakeLog()
        log.extend([b'm-%d' % i for i in range(3)])
        _install_handlers(wired_broker, log, deleted_after_snapshot=True)
        consumer = _make_consumer(wired_broker)
        try:
            with pytest.raises(Errors.SnapshotPartitionNotFoundError):
                list(consumer.bounded_consume())
        finally:
            consumer.close()

    def test_fetch_out_of_range_raises_instead_of_autoreset(self, wired_broker):
        log = _FakeLog()
        log.extend([b'm-%d' % i for i in range(5)])
        _install_handlers(wired_broker, log,
                          fetch_error=Errors.OffsetOutOfRangeError.errno)
        consumer = _make_consumer(wired_broker)
        try:
            with pytest.raises(Errors.OffsetOutOfRangeError):
                list(consumer.bounded_consume())
        finally:
            consumer.close()

    def test_read_committed_boundary_uses_lso(self, wired_broker):
        """With read_committed the snapshot ListOffsets must carry
        READ_COMMITTED isolation so the broker returns the LSO."""
        log = _FakeLog()
        log.extend([b'm-%d' % i for i in range(4)])
        _install_handlers(wired_broker, log,
                          isolation_level=1)  # READ_COMMITTED
        consumer = _make_consumer(wired_broker, isolation_level='read_committed')
        try:
            received = list(consumer.bounded_consume())
            assert [r.offset for r in received] == [0, 1, 2, 3]
            assert consumer.position(TP) == 4
        finally:
            consumer.close()

    def test_paused_partition_rejected(self, wired_broker):
        log = _FakeLog()
        _install_handlers(wired_broker, log)
        consumer = _make_consumer(wired_broker)
        try:
            consumer.pause(TP)
            with pytest.raises(ValueError):
                list(consumer.bounded_consume())
        finally:
            consumer.close()
