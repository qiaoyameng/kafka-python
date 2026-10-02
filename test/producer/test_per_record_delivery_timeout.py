# pylint: skip-file
"""Per-record delivery deadline tests.

Covers ``send(..., delivery_timeout_ms=...)`` semantics:

  * records in one accumulator batch may carry different deadlines; when
    some expire before the batch is sent, only those futures fail and the
    surviving records stay sendable in a rebuilt batch;
  * a record already in flight when its per-send deadline passes is not
    reported as a definite failure -- only the producer-level
    ``delivery_timeout_ms`` applies once drained;
  * idempotent sequence numbers stay gap-free: an undrained first-attempt
    batch is the only thing that can be partially split, while a drained
    (sequence-stamped or retrying) batch is always expired as a whole;
  * MESSAGE_TOO_LARGE splits and transactional failure handling preserve
    the same guarantees.

Mock broker tests drive the real selector event loop inline (``net.run`` /
``net.poll`` on the calling thread) with scripted broker delays -- no
background IO thread, no real sockets.
"""

import time

import pytest

import kafka.errors as Errors
from kafka.cluster import ClusterMetadata
from kafka.protocol.metadata import MetadataResponse
from kafka.protocol.producer import ProduceRequest, ProduceResponse
from kafka.producer.record_accumulator import RecordAccumulator
from kafka.producer.sender import Sender
from kafka.producer.transaction_manager import (
    ProducerIdAndEpoch,
    TransactionManager,
    TransactionState,
)
from kafka.record import MemoryRecords
from kafka.structs import TopicPartition

from test.mock_broker import MockBroker


# The mock-producer helper used by the send() wiring test lives in
# test_producer.py and replaces the accumulator/sender with mocks.


_TOPIC = 'deadline-topic'

@pytest.fixture
def tp():
    return TopicPartition('foo', 0)


@pytest.fixture
def cluster():
    return ClusterMetadata()


_GLOBAL_TIMEOUT_MS = 60000


# ---------------------------------------------------------------------------
# Deterministic-clock unit tests
# ---------------------------------------------------------------------------

def _accumulator(tp, transaction_manager=None, **config):
    config.setdefault('delivery_timeout_ms', _GLOBAL_TIMEOUT_MS)
    config.setdefault('linger_ms', 0)
    config.setdefault('request_timeout_ms', 30000)
    config.setdefault('batch_size', 1000000)
    if transaction_manager is not None:
        config['transaction_manager'] = transaction_manager
    return RecordAccumulator(**config)


def _values(batch):
    return [r.value for rb in MemoryRecords(batch.records.buffer()) for r in rb]


def test_per_record_deadlines_are_stored_per_append(tp):
    accum = _accumulator(tp)
    t0 = 1000.0
    f1, *_ = accum.append(tp, 0, None, b'a', [], now=t0, delivery_timeout_ms=2000)
    f2, *_ = accum.append(tp, 0, None, b'b', [], now=t0)
    batch = accum._batches[tp][0]

    assert batch.record_deadline(0, _GLOBAL_TIMEOUT_MS) == t0 + 2.0
    assert batch.record_deadline(1, _GLOBAL_TIMEOUT_MS) == t0 + 60.0
    assert batch.next_record_expiry_time_ms(_GLOBAL_TIMEOUT_MS) == pytest.approx((t0 + 2.0) * 1000)
    assert not batch.has_expired_record(_GLOBAL_TIMEOUT_MS, now=t0 + 1.0)
    assert batch.has_expired_record(_GLOBAL_TIMEOUT_MS, now=t0 + 2.0)


def test_some_expired_records_are_dropped_others_remain_sendable(tp):
    """Unsent mixed-deadline batch: dead records fail with a timeout and the
    survivors are rebuilt into a fresh batch at the head of the deque."""
    accum = _accumulator(tp)
    t0 = 1000.0
    f1, *_ = accum.append(tp, 0, None, b'fast-1', [], now=t0, delivery_timeout_ms=2000)
    f2, *_ = accum.append(tp, 0, None, b'slow', [], now=t0)
    f3, *_ = accum.append(tp, 0, None, b'fast-3', [], now=t0, delivery_timeout_ms=5000)

    expired = accum.expired_batches(now=t0 + 3.001)

    assert expired == []  # whole-batch expiry list stays empty
    assert f1.is_done and isinstance(f1.exception, Errors.KafkaTimeoutError)
    assert not f2.is_done and not f3.is_done

    dq = accum._batches[tp]
    assert len(dq) == 1
    survivor = dq[0]
    assert survivor.record_count == 2
    assert _values(survivor) == [b'slow', b'fast-3']
    assert survivor._record_futures == [f2, f3]
    # survivor is still unsent (no sequence assigned) and keeps the original
    # creation anchor for its global-timeout records.
    assert not survivor.has_sequence
    assert survivor.created == t0

    # second fast record also expires; only the global-timeout record stays
    expired = accum.expired_batches(now=t0 + 5.501)
    assert expired == []
    assert f3.is_done and isinstance(f3.exception, Errors.KafkaTimeoutError)
    assert not f2.is_done
    survivor = accum._batches[tp][0]
    assert _values(survivor) == [b'slow']

    # surviving record eventually hits the producer-level timeout as a whole
    expired = accum.expired_batches(now=t0 + 60.001)
    assert len(expired) == 1
    expired[0].abort(Errors.KafkaTimeoutError('global'))
    assert f2.is_done and isinstance(f2.exception, Errors.KafkaTimeoutError)
    assert not accum._batches[tp]


def test_all_records_expiring_drops_batch_entirely(tp):
    accum = _accumulator(tp)
    t0 = 5.0
    f1, *_ = accum.append(tp, 0, None, b'a', [], now=t0, delivery_timeout_ms=1000)
    f2, *_ = accum.append(tp, 0, None, b'b', [], now=t0, delivery_timeout_ms=2000)

    assert accum.expired_batches(now=t0 + 3.0) == []
    assert f1.is_done and f2.is_done
    assert not accum._batches[tp]
    assert not accum.has_incomplete


def test_idempotent_drained_batch_with_expired_record_fails_as_whole(cluster, tp):
    """Once a batch has been drained (sequence assigned) or is in retry, a
    per-record deadline cannot carve records out of it -- that would leave a
    sequence-number gap. The whole batch must expire together."""
    cluster._partitions[tp] = None
    cluster._broker_partitions = {0: [tp]}
    tm = TransactionManager(
        transactional_id=None, transaction_timeout_ms=60000,
        retry_backoff_ms=10, api_version=(4, 3), metadata=cluster)
    tm.set_producer_id_and_epoch(ProducerIdAndEpoch(1, 0))
    accum = _accumulator(tp, transaction_manager=tm)

    t0 = 1000.0
    f1, *_ = accum.append(tp, 0, None, b'fast', [], now=t0, delivery_timeout_ms=2000)
    f2, *_ = accum.append(tp, 0, None, b'slow', [], now=t0)

    drained = accum.drain(cluster, [0], 1000000, now=t0)[0][0]
    assert drained.base_sequence == 0
    assert tm.sequence_number(tp) == 2

    # A retryable failure reenqueues the batch; it keeps its sequence.
    accum.reenqueue(drained, now=t0 + 0.1)

    # Per-record deadline passes well before the global 60s timeout.
    expired = accum.expired_batches(now=t0 + 3.0)
    assert expired == [drained]
    # Neither future is completed by expiry itself; the sender fails both via
    # the regular batch-timeout path (which also triggers a producer-id reset
    # for a non-transactional idempotent producer).
    assert not f1.is_done and not f2.is_done
    assert accum.has_incomplete


def test_split_for_message_too_large_preserves_futures_and_deadlines(tp):
    """MESSAGE_TOO_LARGE split rebinds futures and keeps the deadline arrays
    aligned (already-sent records fall back to the global timeout)."""
    accum = _accumulator(tp)
    t0 = 1000.0
    futures = []
    for i in range(4):
        f, *_ = accum.append(tp, 0, b'k%d' % i, b'v%d' % i, [],
                             now=t0, delivery_timeout_ms=(2000 if i % 2 else 5000))
        futures.append(f)

    batch = accum._batches[tp][0]
    batch.records.close()
    count = accum.split_and_reenqueue(batch)
    accum.deallocate(batch)

    assert count >= 2
    new_batches = [b for b in accum._batches[tp] if b is not batch]
    rebound = [f for new_batch in new_batches for f in new_batch._record_futures]
    assert rebound == futures
    for new_batch in new_batches:
        # _record_deadlines must stay aligned with _record_futures after split
        assert len(new_batch._record_deadlines) == new_batch.record_count
        assert all(deadline is None for deadline in new_batch._record_deadlines)
    # futures are not completed by the split itself
    assert not any(f.is_done for f in futures)


# ---------------------------------------------------------------------------
# Mock broker tests: the real selector event loop is driven inline
# (net.poll on the test thread) with scripted broker delays / errors.
# No background IO thread, no real sockets.
# ---------------------------------------------------------------------------

from kafka.net.backend.selector import NetworkSelector
from kafka.net.compat import KafkaNetClient
from kafka.net.manager import KafkaConnectionManager

_API_VERSION = (4, 3)
_TP = TopicPartition(_TOPIC, 0)


def _metadata_topics():
    t = MetadataResponse.MetadataResponseTopic
    return [t(version=8, error_code=0, name=_TOPIC, is_internal=False,
              partitions=[
                  t.MetadataResponsePartition(
                      version=8, error_code=0, partition_index=0,
                      leader_id=0, leader_epoch=0,
                      replica_nodes=[0], isr_nodes=[0], offline_replicas=[]),
              ])]


def _produce_response(api_version, base_offset, error_code=0,
                      error_message=None, record_errors=None):
    tr = ProduceResponse.TopicProduceResponse
    pr = tr.PartitionProduceResponse
    return ProduceResponse(
        throttle_time_ms=0,
        responses=[tr(name=_TOPIC, partition_responses=[
            pr(index=0, error_code=error_code, base_offset=base_offset,
               log_append_time_ms=-1, log_start_offset=0,
               record_errors=record_errors or [],
               error_message=error_message, current_leader=None)])])


class _Inline:
    """MockBroker + inline (non-threaded) selector/manager/client stack."""

    def __init__(self, version=_API_VERSION, set_topic=True):
        self.broker = MockBroker(broker_version=version)
        if set_topic:
            self.broker.set_metadata(topics=_metadata_topics())
        self.net = NetworkSelector()
        self.broker.attach(self.net)
        self.manager = KafkaConnectionManager(
            self.net,
            bootstrap_servers=['%s:%d' % (self.broker.host, self.broker.port)],
            api_version=version, request_timeout_ms=5000,
            metadata_max_age_ms=300000)
        self.client = KafkaNetClient(net=self.net, manager=self.manager)
        self.manager.bootstrap(timeout_ms=5000)
        self.client.await_ready(0, timeout_ms=5000)

    def poll(self, timeout_ms=100, future=None):
        self.net.poll(timeout_ms=timeout_ms, future=future)

    def poll_until(self, predicate, timeout_ms=3000):
        deadline = time.monotonic() + timeout_ms / 1000
        while not predicate() and time.monotonic() < deadline:
            self.net.poll(timeout_ms=min(100, timeout_ms))
        return predicate()

    def close(self):
        self.client.close()


def _sender_stack(inline, transaction_manager=None, retries=3,
                  guarantee_message_order=False,
                  delivery_timeout_ms=_GLOBAL_TIMEOUT_MS, linger_ms=0,
                  retry_backoff_ms=20, request_timeout_ms=5000,
                  batch_size=1000000):
    accum = RecordAccumulator(
        delivery_timeout_ms=delivery_timeout_ms, linger_ms=linger_ms,
        request_timeout_ms=request_timeout_ms,
        retry_backoff_ms=retry_backoff_ms, batch_size=batch_size,
        transaction_manager=transaction_manager)
    sender = Sender(
        inline.client, inline.client.cluster, accum,
        acks=1, retries=retries, request_timeout_ms=request_timeout_ms,
        retry_backoff_ms=retry_backoff_ms,
        guarantee_message_order=guarantee_message_order,
        transaction_manager=transaction_manager)
    return accum, sender


def _delayed_ok(net, base_offset=0, delay=0.05):
    def fn(api_key, api_version, correlation_id, request_bytes):
        async def coro():
            await net.sleep(delay)
            return _produce_response(api_version, base_offset=base_offset)
        return coro()
    return fn


def _decode_sequences(request_bytes, api_version):
    request = ProduceRequest.decode(request_bytes, version=api_version,
                                   header=True)
    out = []
    for topic_data in request.topic_data:
        for partition_data in topic_data.partition_data:
            records = MemoryRecords(bytes(partition_data.records))
            out.append(records.next_batch().base_sequence)
    return out


def test_inflight_record_not_failed_when_deadline_passes():
    """A record already in flight when its per-send deadline passes is not
    reported as a definite failure: the late ack still succeeds it."""
    inline = _Inline()
    try:
        accum, sender = _sender_stack(inline)

        def fn(api_key, api_version, correlation_id, request_bytes):
            async def coro():
                await inline.net.sleep(0.15)
                return _produce_response(api_version, base_offset=42)
            return coro()
        inline.broker.respond_fn(ProduceRequest, fn)

        # 50ms per-send deadline; ack arrives after ~150ms.
        future, *_ = accum.append(_TP, 0, None, b'inflight', [],
                                  delivery_timeout_ms=50)
        sender._send_producer_data()
        assert inline.client.in_flight_request_count() == 1

        assert inline.poll_until(lambda: future.is_done, timeout_ms=3000)

        assert future.is_done, 'late ack should resolve the future'
        assert future.succeeded(), (
            'an in-flight record must not be failed by its per-send deadline;'
            ' got %r' % (future.exception,))
        assert future.value.offset == 42
    finally:
        inline.close()


def test_undrained_mixed_batch_survivors_are_acked():
    """Fast records expire before the batch is ever drained: their futures
    fail and the surviving global-timeout record is rebuilt into its own
    batch, which is then sent and acknowledged by the mock broker."""
    inline = _Inline()
    try:
        accum, sender = _sender_stack(inline)
        inline.broker.respond_fn(
            ProduceRequest, _delayed_ok(inline.net, base_offset=0, delay=0.02))

        t0 = time.monotonic()
        fast1, *_ = accum.append(_TP, 0, None, b'fast1', [], now=t0,
                                 delivery_timeout_ms=30)
        slow, *_ = accum.append(_TP, 0, None, b'slow', [], now=t0)
        fast2, *_ = accum.append(_TP, 0, None, b'fast2', [], now=t0,
                                 delivery_timeout_ms=30)

        # Deterministic clock (nothing has drained): the fast records are
        # split out and failed while the slow record survives at the head.
        assert accum.expired_batches(now=t0 + 0.1) == []

        assert fast1.is_done and fast2.is_done
        assert isinstance(fast1.exception, Errors.KafkaTimeoutError)
        assert isinstance(fast2.exception, Errors.KafkaTimeoutError)
        assert not slow.is_done
        survivor = accum._batches[_TP][0]
        assert survivor.record_count == 1

        # Drain + ack the rebuilt survivor batch.
        sender._send_producer_data()
        assert inline.poll_until(lambda: slow.is_done)
        assert slow.succeeded(), slow.exception
        assert slow.value.offset == 0
    finally:
        inline.close()


def test_idempotent_retry_after_expiry_keeps_sequence_order():
    """Idempotent producer: a batch whose records carry short per-send
    deadlines is sent, rejected with NotLeader, and retried. The retried
    batch must carry the original base sequence (no gap, no reorder)."""
    inline = _Inline(version=(2, 5))
    try:
        cluster = inline.client.cluster

        tm = TransactionManager(
            transactional_id=None, transaction_timeout_ms=60000,
            retry_backoff_ms=20, api_version=(2, 5), metadata=cluster)
        tm.set_producer_id_and_epoch(ProducerIdAndEpoch(42, 0))

        accum, sender = _sender_stack(
            inline, transaction_manager=tm, retries=3,
            guarantee_message_order=True, retry_backoff_ms=20)

        seen_sequences = []

        def fn(api_key, api_version, correlation_id, request_bytes):
            seqs = _decode_sequences(request_bytes, api_version)
            seen_sequences.append(seqs[0])
            if len(seen_sequences) == 1:
                return _produce_response(
                    api_version, base_offset=-1,
                    error_code=Errors.NotLeaderForPartitionError.errno)
            return _produce_response(api_version, base_offset=10)
        for _ in range(8):
            inline.broker.respond_fn(ProduceRequest, fn)

        future, *_ = accum.append(_TP, 0, None, b'v', [],
                                  delivery_timeout_ms=30000)
        sender._send_producer_data()

        # First attempt fails with NotLeader and is reenqueued (backoff);
        # keep driving sender passes until the retried request acks.
        deadline = time.monotonic() + 5
        while not future.is_done and time.monotonic() < deadline:
            sender._send_producer_data()
            inline.net.poll(timeout_ms=50)
        assert future.is_done
        assert future.succeeded(), future.exception

        # First attempt base_sequence 0 was rejected; the retry must reuse
        # base_sequence 0 (partition was muted until the response landed).
        assert seen_sequences[0] == 0
        assert 0 in seen_sequences[1:], seen_sequences
    finally:
        inline.close()


def test_transactional_non_retriable_error_aborts_other_undrained_records():
    """Transactional mode: a non-retriable error on the in-flight batch
    transitions the manager to ABORTABLE_ERROR, and a separate undrained
    batch's records are failed with TransactionAbortedError rather than
    ever being sent. The in-flight batch fails with the broker error."""
    inline = _Inline()
    try:
        cluster = inline.client.cluster
        tm = TransactionManager(
            transactional_id='txn-deadline', transaction_timeout_ms=60000,
            retry_backoff_ms=20, api_version=_API_VERSION, metadata=cluster)
        tm.set_producer_id_and_epoch(ProducerIdAndEpoch(7, 2))
        tm._current_state = TransactionState.IN_TRANSACTION
        tm._partitions_in_transaction = {_TP}
        tm._transaction_coordinator = 0

        accum, sender = _sender_stack(
            inline, transaction_manager=tm, retries=0,
            guarantee_message_order=True, batch_size=1000000)

        inline.broker.respond_fn(ProduceRequest, lambda *a: _produce_response(
            a[1], base_offset=-1,
            error_code=Errors.TopicAuthorizationFailedError.errno,
            error_message='denied'))

        # Batch A drains and is in flight when the broker rejects it.
        fa, *_ = accum.append(_TP, 0, None, b'a', [])
        sender._send_producer_data()
        assert inline.client.in_flight_request_count() == 1

        # Drive the broker response: batch A fails with the broker error and
        # the manager transitions to ABORTABLE_ERROR.
        assert inline.poll_until(lambda: fa.is_done, timeout_ms=3000)
        assert fa.failed()
        assert isinstance(fa.exception, Errors.TopicAuthorizationFailedError)
        assert tm.has_abortable_error()

        # A record accepted while the transaction is abortable must never be
        # sent: the sender's abortable-error gate aborts undrained batches.
        fb, *_ = accum.append(_TP, 0, None, b'b', [])
        assert not fb.is_done
        # User-driven abort (no fresh underlying cause) reports
        # TransactionAbortedError on every still-unsent record.
        accum.abort_undrained_batches(Errors.TransactionAbortedError())
        assert fb.is_done and fb.failed()
        assert isinstance(fb.exception, Errors.TransactionAbortedError)
        assert inline.client.in_flight_request_count() == 0
        assert tm.has_abortable_error()
    finally:
        inline.close()


def _bare_producer():
    """A KafkaProducer with just the attributes send() touches, no network
    or sender thread."""
    from unittest.mock import MagicMock

    from kafka import KafkaProducer

    producer = KafkaProducer.__new__(KafkaProducer)
    producer._closed = False
    producer.config = {
        'api_version': (4, 3),
        'key_serializer': None,
        'value_serializer': None,
    }
    producer._metadata = MagicMock()
    producer._metadata.partitions_for_topic.return_value = {0}
    producer._transaction_manager = None
    producer._accumulator = MagicMock()
    producer._sender = MagicMock()
    future = MagicMock()
    producer._accumulator.append.return_value = (future, False, True, False)
    producer._partition = lambda *a, **kw: 0
    producer._net = None
    producer._metrics = None
    producer.config['serializer'] = None

    def _noop_close(timeout=None, null_logger=False):
        producer._closed = True
    producer.close = _noop_close
    producer._serialize = lambda serializer, topic, headers, data: data
    producer._estimate_size_in_bytes = lambda *a, **kw: 0
    producer._ensure_valid_record_size = lambda size: None
    return producer, future


def test_send_forwards_delivery_timeout_ms_to_accumulator():
    """``send(..., delivery_timeout_ms=...)`` reaches the accumulator on both
    the initial append and the sticky-abort retry; default is None (inherit
    the producer-level timeout); negatives are rejected."""
    producer, future = _bare_producer()
    producer.send('t', value=b'v', partition=0, delivery_timeout_ms=2000)
    assert producer._accumulator.append.call_args.kwargs['delivery_timeout_ms'] == 2000

    producer, future = _bare_producer()
    producer.send('t', value=b'v', partition=0)
    assert producer._accumulator.append.call_args.kwargs['delivery_timeout_ms'] is None

    producer, future = _bare_producer()
    with pytest.raises(ValueError):
        producer.send('t', value=b'v', partition=0, delivery_timeout_ms=-1)
