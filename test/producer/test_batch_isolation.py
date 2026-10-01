# pylint: skip-file
"""Tests for the KafkaProducer batch isolation key.

Records sent with different ``batch_isolation`` values must never share a
record batch (and hence never share a compressed batch), while still being
delivered to the same partition in send order. The isolation value is a
local batching hint only -- it is never written to the wire.
"""

import threading
import time
from unittest.mock import MagicMock

import pytest

import kafka.errors as Errors
from kafka import KafkaProducer
from kafka.cluster import ClusterMetadata
from kafka.partitioner import StickyPartitioner
from kafka.producer.future import FutureProduceResult, FutureRecordMetadata
from kafka.producer.producer_batch import ProducerBatch
from kafka.producer.record_accumulator import RecordAccumulator
from kafka.producer.sender import Sender
from kafka.protocol.metadata import MetadataResponse
from kafka.protocol.producer import ProduceRequest, ProduceResponse
from kafka.record.memory_records import MemoryRecords, MemoryRecordsBuilder
from kafka.structs import TopicPartition


@pytest.fixture
def tp():
    return TopicPartition('foo', 0)


@pytest.fixture
def cluster(tp, mocker):
    metadata = ClusterMetadata()
    mocker.patch.object(metadata, 'leader_for_partition', return_value=0)
    mocker.patch.object(metadata, 'partitions_for_broker', return_value=[tp])
    return metadata


def _parse_batch(batch):
    """Parse a ProducerBatch back into a list of (key, value) pairs."""
    batch.records.close()
    parsed = []
    for record_batch in MemoryRecords(batch.records.buffer()):
        for record in record_batch:
            parsed.append((record.key, record.value))
    return parsed


# ---------------------------------------------------------------------------
# RecordAccumulator isolation semantics
# ---------------------------------------------------------------------------


def test_same_isolation_shares_batch(tp):
    accum = RecordAccumulator()
    f1, _, new1, _ = accum.append(tp, 0, b'k', b'v1', [], now=0, isolation='A')
    f2, _, new2, _ = accum.append(tp, 0, b'k', b'v2', [], now=0, isolation='A')
    assert new1 is True
    assert new2 is False, 'same isolation value must share the open batch'
    assert len(accum._batches[tp]) == 1
    batch = accum._batches[tp][0]
    assert batch.isolation == 'A'
    assert _parse_batch(batch) == [(b'k', b'v1'), (b'k', b'v2')]


def test_isolation_switch_seals_current_batch(tp):
    accum = RecordAccumulator()
    accum.append(tp, 0, b'k', b'v1', [], now=0, isolation='A')
    accum.append(tp, 0, b'k', b'v2', [], now=0, isolation='A')
    f3, _, new3, _ = accum.append(tp, 0, b'k', b'v3', [], now=0, isolation='B')
    assert new3 is True, 'isolation switch must seal the current batch'
    dq = accum._batches[tp]
    assert len(dq) == 2
    assert dq[0].isolation == 'A'
    assert dq[1].isolation == 'B'
    assert _parse_batch(dq[0]) == [(b'k', b'v1'), (b'k', b'v2')]
    assert _parse_batch(dq[1]) == [(b'k', b'v3')]


def test_no_merge_past_intermediate_isolation(tp):
    """A -> B -> A must produce three batches: the second A record may not
    be merged into the first batch past the intervening B record."""
    accum = RecordAccumulator()
    accum.append(tp, 0, b'k', b'a1', [], now=0, isolation='A')
    accum.append(tp, 0, b'k', b'b1', [], now=0, isolation='B')
    accum.append(tp, 0, b'k', b'a2', [], now=0, isolation='A')
    dq = accum._batches[tp]
    assert len(dq) == 3
    assert [batch.isolation for batch in dq] == ['A', 'B', 'A']
    assert _parse_batch(dq[0]) == [(b'k', b'a1')]
    assert _parse_batch(dq[1]) == [(b'k', b'b1')]
    assert _parse_batch(dq[2]) == [(b'k', b'a2')]


def test_default_no_isolation_unchanged(tp):
    """Without an isolation key, records keep sharing batches (and mix with
    isolation=None records) exactly as before."""
    accum = RecordAccumulator()
    accum.append(tp, 0, b'k', b'v1', [], now=0)
    f2, _, new2, _ = accum.append(tp, 0, b'k', b'v2', [], now=0, isolation=None)
    assert new2 is False
    assert len(accum._batches[tp]) == 1
    assert accum._batches[tp][0].isolation is None
    # But an explicit isolation value still seals the default batch.
    _, _, new3, _ = accum.append(tp, 0, b'k', b'v3', [], now=0, isolation='A')
    assert new3 is True
    assert len(accum._batches[tp]) == 2


def test_isolation_batches_drain_in_send_order(tp, cluster):
    accum = RecordAccumulator(linger_ms=0)
    plan = ['A', 'A', 'B', 'A']
    for i, iso in enumerate(plan):
        accum.append(tp, 0, b'k', ('v%d' % i).encode(), [], now=0, isolation=iso)
    ready, _, _ = accum.ready(cluster, now=0)
    assert ready == {0}, 'sealed batches must not wait for linger'
    drained = accum.drain(cluster, ready, 2147483647, now=0)[0]
    assert len(drained) == 1  # one batch per partition per drain
    assert _parse_batch(drained[0]) == [(b'k', b'v0'), (b'k', b'v1')]
    # Drain the rest one at a time; order must match send order.
    values = [b'v0', b'v1']
    for _ in range(2):
        ready, _, _ = accum.ready(cluster, now=0)
        drained = accum.drain(cluster, ready, 2147483647, now=0)[0]
        assert len(drained) == 1
        values.extend(value for _key, value in _parse_batch(drained[0]))
    assert values == [b'v0', b'v1', b'v2', b'v3']


def test_isolation_survives_retry_reenqueue(tp, cluster):
    """A retried batch is re-enqueued at the front with its isolation value
    intact; later appends still only merge with the tail batch."""
    accum = RecordAccumulator(linger_ms=0)
    accum.append(tp, 0, b'k', b'a1', [], now=0, isolation='A')
    accum.append(tp, 0, b'k', b'b1', [], now=0, isolation='B')
    ready, _, _ = accum.ready(cluster, now=0)
    drained = accum.drain(cluster, ready, 2147483647, now=0)[0]
    assert len(drained) == 1
    batch_a = drained[0]
    assert batch_a.isolation == 'A'

    accum.reenqueue(batch_a, now=1)
    dq = accum._batches[tp]
    assert len(dq) == 2
    assert dq[0] is batch_a and dq[0].isolation == 'A'
    assert dq[1].isolation == 'B'

    # A new 'A' record must not join the retried 'A' batch at the front --
    # it goes behind the 'B' batch, preserving send order.
    accum.append(tp, 0, b'k', b'a2', [], now=2, isolation='A')
    assert len(dq) == 3
    assert [batch.isolation for batch in dq] == ['A', 'B', 'A']


def test_same_isolation_full_batch_rolls_over(tp):
    """batch_size still applies: a full batch rolls over to a new batch
    carrying the same isolation value."""
    accum = RecordAccumulator(batch_size=64)
    f1, _, new1, _ = accum.append(tp, 0, b'key', b'v1', [], now=0, isolation='A')
    f2, _, new2, _ = accum.append(tp, 0, b'key', b'v2', [], now=0, isolation='A')
    assert new1 is True
    assert new2 is True, 'full batch must roll over to a new batch'
    dq = accum._batches[tp]
    assert len(dq) == 2
    assert all(batch.isolation == 'A' for batch in dq)


def test_isolation_switch_with_abort_on_new_batch(tp):
    """KIP-480: an isolation switch that would need a new batch honors
    abort_on_new_batch instead of allocating one."""
    accum = RecordAccumulator()
    accum.append(tp, 0, b'k', b'v1', [], now=0, isolation='A')
    future, full, new, abort = accum.append(
        tp, 0, b'k', b'v2', [], now=0, isolation='B', abort_on_new_batch=True)
    assert abort is True
    assert future is None
    assert len(accum._batches[tp]) == 1


def test_split_and_reenqueue_preserves_isolation(tp):
    accum = RecordAccumulator()
    records = MemoryRecordsBuilder(magic=2, compression_type=0, batch_size=100000)
    batch = ProducerBatch(tp, records, isolation='A')
    for i in range(4):
        batch.try_append(0, b'k', ('v%d' % i).encode(), [])
    batch.records.close()
    accum._incomplete.add(batch)

    accum.split_and_reenqueue(batch)
    accum.deallocate(batch)

    dq = accum._batches[tp]
    assert len(dq) == 2
    assert all(new_batch.isolation == 'A' for new_batch in dq)
    values = []
    for new_batch in dq:
        values.extend(value for _key, value in _parse_batch(new_batch))
    assert values == [b'v0', b'v1', b'v2', b'v3']


# ---------------------------------------------------------------------------
# Bounded per-partition buffering
# ---------------------------------------------------------------------------


def test_max_open_batches_blocks_then_times_out(tp):
    """With the per-partition buffer full of sealed batches, an append that
    needs a new batch blocks and then raises KafkaTimeoutError."""
    accum = RecordAccumulator(max_open_batches_per_partition=2, max_block_ms=50)
    accum.append(tp, 0, b'k', b'v1', [], now=0, isolation='A')
    accum.append(tp, 0, b'k', b'v2', [], now=0, isolation='B')
    assert len(accum._batches[tp]) == 2
    start = time.monotonic()
    with pytest.raises(Errors.KafkaTimeoutError):
        accum.append(tp, 0, b'k', b'v3', [], now=0, isolation='C')
    assert time.monotonic() - start < 5


def test_max_open_batches_matching_isolation_still_appends(tp):
    """The buffer bound only gates *new* batch allocation; appending to the
    open tail batch with a matching isolation value always works."""
    accum = RecordAccumulator(max_open_batches_per_partition=2, max_block_ms=50)
    accum.append(tp, 0, b'k', b'v1', [], now=0, isolation='A')
    accum.append(tp, 0, b'k', b'v2', [], now=0, isolation='B')
    future, _, new_batch, _ = accum.append(tp, 0, b'k', b'v3', [], now=0, isolation='B')
    assert future is not None
    assert new_batch is False
    assert len(accum._batches[tp]) == 2


def test_max_open_batches_unblocks_on_drain(tp, cluster):
    """A blocked append proceeds once the sender drains a batch."""
    accum = RecordAccumulator(linger_ms=0, max_open_batches_per_partition=2,
                              max_block_ms=10000)
    accum.append(tp, 0, b'k', b'v1', [], now=0, isolation='A')
    accum.append(tp, 0, b'k', b'v2', [], now=0, isolation='B')

    def drainer():
        time.sleep(0.05)
        ready, _, _ = accum.ready(cluster, now=0)
        accum.drain(cluster, ready, 2147483647, now=0)

    thread = threading.Thread(target=drainer)
    thread.start()
    future, _, new_batch, _ = accum.append(tp, 0, b'k', b'v3', [], now=0, isolation='C')
    thread.join()
    assert future is not None
    assert new_batch is True
    assert len(accum._batches[tp]) == 2  # one drained, one added


def test_max_open_batches_abort_on_new_batch_never_blocks(tp):
    """KIP-480 callers rotate the sticky partition instead of blocking."""
    accum = RecordAccumulator(max_open_batches_per_partition=1, max_block_ms=50)
    accum.append(tp, 0, b'k', b'v1', [], now=0, isolation='A')
    future, _, _, abort = accum.append(
        tp, 0, b'k', b'v2', [], now=0, isolation='B', abort_on_new_batch=True)
    assert abort is True
    assert future is None


# ---------------------------------------------------------------------------
# KafkaProducer.send plumbing
# ---------------------------------------------------------------------------


def _bare_producer(partitioner):
    """A KafkaProducer with no network/IO threads: constructed via __new__
    with the collaborator attributes send() touches swapped for mocks."""
    producer = KafkaProducer.__new__(KafkaProducer)
    producer._closed = False
    producer.config = dict(KafkaProducer.DEFAULT_CONFIG)
    producer.config['api_version'] = (2, 5)
    producer.config['partitioner'] = partitioner
    producer._metadata = MagicMock()
    producer._metadata.topics.return_value = {'t'}
    producer._metadata.partitions_for_topic.return_value = set(range(20))
    producer._metadata.available_partitions_for_topic.return_value = set(range(20))
    producer._accumulator = MagicMock()
    producer._sender = MagicMock()
    producer._transaction_manager = None
    return producer


def _success_result():
    return (FutureRecordMetadata(
                FutureProduceResult(TopicPartition('t', 0)),
                0, 0, 0, 0, 0, 0),
            False, True, False)


def test_send_passes_isolation_to_accumulator():
    partitioner = MagicMock(spec=StickyPartitioner)
    partitioner.partition.return_value = 0

    producer = _bare_producer(partitioner)
    producer._accumulator.append.return_value = _success_result()
    producer.send('t', value=b'v', batch_isolation='tenant-1')
    kwargs = producer._accumulator.append.call_args.kwargs
    assert kwargs.get('isolation') == 'tenant-1'


def test_send_with_isolation_skips_sticky_rotation():
    """Isolation-keyed records must stay on their chosen partition: the
    KIP-480 sticky abort/rotate path is bypassed like for keyed records."""
    partitioner = MagicMock(spec=StickyPartitioner)
    partitioner.partition.return_value = 0

    producer = _bare_producer(partitioner)
    producer._accumulator.append.return_value = _success_result()
    producer.send('t', value=b'v', batch_isolation='tenant-1')
    partitioner.on_new_batch.assert_not_called()
    assert producer._accumulator.append.call_count == 1
    kwargs = producer._accumulator.append.call_args.kwargs
    assert kwargs.get('abort_on_new_batch') is False


def test_send_without_isolation_defaults_to_none():
    partitioner = MagicMock(spec=StickyPartitioner)
    partitioner.partition.return_value = 0

    producer = _bare_producer(partitioner)
    producer._accumulator.append.return_value = _success_result()
    producer.send('t', key=b'k', value=b'v')
    kwargs = producer._accumulator.append.call_args.kwargs
    assert kwargs.get('isolation') is None


# ---------------------------------------------------------------------------
# End-to-end against a MockBroker: parse the actual wire batches
# ---------------------------------------------------------------------------


class _WireHarness:
    """A real Sender + RecordAccumulator driven inline against a MockBroker.

    The net backend is not started on an IO thread; ``drive()`` steps the
    sender and pumps the selector loop on the calling thread, so every
    ProduceRequest/Response exchange is deterministic.
    """

    TOPIC = 'iso-topic'

    def __init__(self, net, broker, manager, client):
        self.net = net
        self.broker = broker
        self.client = client
        self.accumulator = RecordAccumulator(
            compression_attrs=1,  # gzip: exercise real compressed batches
            linger_ms=0,
            retry_backoff_ms=10,
            request_timeout_ms=5000,
            delivery_timeout_ms=30000,
            message_version=2,
        )
        self.sender = Sender(client, client.cluster, self.accumulator,
                             acks=-1, retries=10, retry_backoff_ms=10,
                             request_timeout_ms=5000,
                             guarantee_message_order=True)
        self.wire_requests = []  # per ProduceRequest: list of decoded batches
        self.next_offset = 0

    @property
    def tp(self):
        return TopicPartition(self.TOPIC, 0)

    def install_handler(self, respond_for):
        """Record every ProduceRequest's decoded batches and reply via
        ``respond_for(call_count, num_records, api_version)``."""
        def handle(api_key, api_version, correlation_id, request_bytes):
            batches = _decode_wire_batches(request_bytes, api_version)
            self.wire_requests.append(batches)
            num_records = sum(len(batch) for batch in batches)
            return respond_for(len(self.wire_requests), num_records, api_version)
        self.broker.respond_always(ProduceRequest, handle)

    def success_response(self, num_records, api_version):
        base = self.next_offset
        self.next_offset += num_records
        return _produce_response(api_version, 0, base)

    def send_plan(self, plan):
        """Append one record per isolation value in ``plan``; values encode
        the send index and isolation value as b'<index>:<isolation>'."""
        futures = []
        for i, iso in enumerate(plan):
            future, _, _, _ = self.accumulator.append(
                self.tp, 0, None, ('%d:%s' % (i, iso)).encode(), [],
                isolation=iso)
            futures.append(future)
        return futures

    def drive(self, max_rounds=100):
        """Step the sender until every batch has completed."""
        for _ in range(max_rounds):
            if not self.accumulator.has_incomplete:
                return
            self.sender._send_producer_data()

            async def _pump():
                await self.net.sleep(0.005)
                while self.client.in_flight_request_count() > 0:
                    await self.net.sleep(0.001)
            self.net.run(_pump(), timeout_ms=10000)
        raise AssertionError('batches never completed')


def _decode_wire_batches(request_bytes, api_version):
    """Decode a ProduceRequest into a list of batches; each batch is a list
    of (key, value) pairs. The record parser transparently decompresses, so
    this reflects the actual compressed batches the broker would append."""
    request = ProduceRequest.decode(request_bytes, version=api_version, header=True)
    batches = []
    for topic_data in request.topic_data:
        for partition_data in topic_data.partition_data:
            records = MemoryRecords(bytes(partition_data.records))
            while records.has_next():
                record_batch = records.next_batch()
                batches.append([(rec.key, rec.value) for rec in record_batch])
    return batches


def _produce_response(version, error_code, base_offset):
    Topic = ProduceResponse.TopicProduceResponse
    Partition = Topic.PartitionProduceResponse
    return ProduceResponse(
        throttle_time_ms=0,
        responses=[
            Topic(name=_WireHarness.TOPIC, partition_responses=[
                Partition(index=0, error_code=error_code,
                          base_offset=base_offset, log_append_time_ms=-1,
                          log_start_offset=0, record_errors=[],
                          error_message=None, current_leader=None),
            ]),
        ],
    )


def _make_metadata_topic(version):
    Topic = MetadataResponse.MetadataResponseTopic
    Partition = Topic.MetadataResponsePartition
    return Topic(version=version, error_code=0, name=_WireHarness.TOPIC,
                 is_internal=False,
                 partitions=[
                     Partition(version=version, error_code=0,
                               partition_index=0, leader_id=0,
                               leader_epoch=0,
                               replica_nodes=[0], isr_nodes=[0],
                               offline_replicas=[]),
                 ])


@pytest.mark.parametrize('broker', [(2, 5)], indirect=True)
class TestBatchIsolationWire:
    """Drive isolation-keyed records through a real Sender to a MockBroker,
    decode the ProduceRequests that actually go out, and verify batch
    composition, record order, and Future completion -- including under
    retries and MESSAGE_TOO_LARGE batch splits."""

    @pytest.fixture
    def wire(self, net, broker, manager, client):
        broker.set_metadata(topics=[_make_metadata_topic(version=8)])
        manager.bootstrap(timeout_ms=5000)
        return _WireHarness(net, broker, manager, client)

    @staticmethod
    def _wire_indices(wire_requests):
        """Flatten decoded requests into per-batch lists of record indices."""
        return [[int(value.split(b':')[0]) for _key, value in batch]
                for request in wire_requests for batch in request]

    @staticmethod
    def _assert_homogeneous(wire_requests):
        for request in wire_requests:
            for batch in request:
                isos = {value.split(b':')[1] for _key, value in batch}
                assert len(isos) == 1, ('batch mixes isolation values: %r' % (batch,))

    def test_isolation_groups_wire_batches_and_preserves_order(self, wire):
        wire.install_handler(
            lambda _call, num_records, api_version: wire.success_response(num_records, api_version))
        plan = ['P', 'A', 'A', 'B', 'B', 'A', 'C', 'C']
        futures = wire.send_plan(plan)
        wire.drive()

        # One batch per isolation run, in send order; the second 'A' record
        # is not merged past the intervening 'B' records.
        assert self._wire_indices(wire.wire_requests) == [
            [0], [1, 2], [3, 4], [5], [6, 7]]
        self._assert_homogeneous(wire.wire_requests)
        # All futures completed, with offsets assigned in send order.
        assert all(future.succeeded() for future in futures)
        assert [future.value.offset for future in futures] == list(range(len(plan)))

    def test_retry_preserves_isolation_and_order(self, wire):
        """A retriable error re-sends the failed batch intact (same records,
        same isolation) ahead of any later batch."""
        def respond_for(call_count, num_records, api_version):
            if call_count == 1:
                return _produce_response(
                    api_version, Errors.NotLeaderForPartitionError.errno, -1)
            return wire.success_response(num_records, api_version)

        wire.install_handler(respond_for)
        plan = ['P', 'A', 'A', 'B']
        futures = wire.send_plan(plan)
        wire.drive()

        # The first batch appears twice: the failed attempt and its retry,
        # carrying the same records, before any later batch.
        assert self._wire_indices(wire.wire_requests) == [[0], [0], [1, 2], [3]]
        self._assert_homogeneous(wire.wire_requests)
        assert all(future.succeeded() for future in futures)
        assert [future.value.offset for future in futures] == list(range(len(plan)))

    def test_split_preserves_isolation_and_order(self, wire):
        """MESSAGE_TOO_LARGE splits the oversized batch into isolation-
        homogeneous halves that are re-sent in order."""
        too_large_sent = [False]

        def respond_for(_call, num_records, api_version):
            if num_records > 2 and not too_large_sent[0]:
                too_large_sent[0] = True
                return _produce_response(
                    api_version, Errors.MessageSizeTooLargeError.errno, -1)
            return wire.success_response(num_records, api_version)

        wire.install_handler(respond_for)
        plan = ['P', 'A', 'A', 'A', 'A', 'B', 'B']
        futures = wire.send_plan(plan)
        wire.drive()

        assert too_large_sent[0], 'split path was never exercised'
        # The 4-record 'A' batch is rejected and re-sent as two halves.
        assert self._wire_indices(wire.wire_requests) == [
            [0], [1, 2, 3, 4], [1, 2], [3, 4], [5, 6]]
        self._assert_homogeneous(wire.wire_requests)
        assert all(future.succeeded() for future in futures)
        assert [future.value.offset for future in futures] == list(range(len(plan)))
