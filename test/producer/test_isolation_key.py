# pylint: skip-file
"""End-to-end coverage for the producer's optional batch isolation key.

These tests use the MockBroker (no real network) and decode the actual
records carried by each wire ProduceRequest, so the assertions are about
the real compression-batch boundaries -- not just accumulator internals.

They exercise:

* different isolation keys never share a wire batch, even under linger;
* a key reappearing after an intervening different key never merges across
  it, and records arrive at the broker in send() order on one partition;
* a retriable broker error mid-stream keeps ordering intact and still keeps
  keys unmerged after the retry;
* MESSAGE_TOO_LARGE split + retry keeps the isolation invariant, record
  order, and Future completion (success and failure).
"""

import pytest

from kafka import KafkaProducer
import kafka.errors as Errors
from kafka.protocol.metadata import MetadataResponse
from kafka.protocol.producer import ProduceRequest, ProduceResponse
from kafka.record import MemoryRecords
from test.mock_broker import MockBroker

# The full-producer lifecycle exercised here needs the selector's
# cross-thread IO wakeup; the cross_thread_wakeup fixture swaps in an
# os.pipe()-based wakeup channel only on sandboxes where socketpair
# cross-thread writes are forbidden (no-op elsewhere).

_TOPIC = 'iso'


def _metadata_topic():
    Topic = MetadataResponse.MetadataResponseTopic
    Partition = Topic.MetadataResponsePartition
    return Topic(version=8, error_code=0, name=_TOPIC, is_internal=False,
                 partitions=[
                     Partition(version=8, error_code=0, partition_index=0,
                               leader_id=0, leader_epoch=0,
                               replica_nodes=[0], isr_nodes=[0],
                               offline_replicas=[]),
                 ])


def _success_response(api_version, base_offset=0):
    Topic = ProduceResponse.TopicProduceResponse
    Partition = Topic.PartitionProduceResponse
    return ProduceResponse(
        throttle_time_ms=0,
        responses=[
            Topic(name=_TOPIC, partition_responses=[
                Partition(index=0, error_code=0, base_offset=base_offset,
                          log_append_time_ms=-1, log_start_offset=0,
                          record_errors=[], error_message=None,
                          current_leader=None),
            ]),
        ],
    )


def _error_response(api_version, error_cls):
    Topic = ProduceResponse.TopicProduceResponse
    Partition = Topic.PartitionProduceResponse
    return ProduceResponse(
        throttle_time_ms=0,
        responses=[
            Topic(name=_TOPIC, partition_responses=[
                Partition(index=0, error_code=error_cls.errno, base_offset=-1,
                          log_append_time_ms=-1, log_start_offset=0,
                          record_errors=[], error_message=None,
                          current_leader=None),
            ]),
        ],
    )


def parse_wire_batches(request_bytes, api_version):
    """Decode a ProduceRequest into the list of wire batches it carries
    for partition 0. Returns a list of batches; each batch is a list of
    (key, value) tuples in wire order.

    A producer batch compresses all its records as one unit, so this is the
    exact granularity at which the isolation constraint must hold.
    """
    request = ProduceRequest.decode(request_bytes, version=api_version,
                                    header=True)
    batches = []
    for topic_data in request.topic_data:
        for partition_data in topic_data.partition_data:
            records = MemoryRecords(bytes(partition_data.records))
            for record_batch in records:
                batches.append([(r.key, r.value) for r in record_batch])
    return batches


def _make_broker():
    broker = MockBroker(broker_version=(4, 3))
    broker.set_metadata(topics=[_metadata_topic()])
    return broker


def _make_producer(broker, **overrides):
    configs = dict(
        api_version=(4, 3),
        enable_idempotence=False,
        linger_ms=50,                 # batching is attractive...
        batch_size=1 << 20,          # ...and room is plentiful, so isolation
        retry_backoff_ms=10,         # is the *only* reason to split batches
        request_timeout_ms=10000,
        delivery_timeout_ms=60000,
        max_in_flight_requests_per_connection=1,
        retries=5,
    )
    configs.update(overrides)
    return KafkaProducer(
        kafka_client=broker.client_factory(),
        bootstrap_servers=['%s:%d' % (broker.host, broker.port)],
        **configs)


def test_isolation_keys_never_share_wire_batch(cross_thread_wakeup):
    """Two keys interleaved under linger must appear on the wire as
    homogeneous, FIFO-ordered batches."""
    broker = _make_broker()
    wire_batches = []

    def handler(api_key, api_version, correlation_id, request_bytes):
        wire_batches.append(parse_wire_batches(request_bytes, api_version))
        return _success_response(api_version)

    for _ in range(64):
        broker.respond_fn(ProduceRequest, handler)

    producer = _make_producer(broker)
    try:
        # A, A, B, A  -- without isolation the linger window would lump all
        # four into one compressed batch.
        sends = [
            (b'a1', 'A'), (b'a2', 'A'), (b'b1', 'B'), (b'a3', 'A'),
        ]
        futures = [producer.send(_TOPIC, value=v, partition=0,
                                 isolation_key=k)
                   for v, k in sends]
        for f in futures:
            f.get(timeout=10)
    finally:
        producer.close(timeout=10)

    # Flatten the per-request batches in arrival order.
    flat = [batch for request_batches in wire_batches
            for batch in request_batches]
    assert flat, 'expected at least one ProduceRequest'

    # Every wire batch is homogeneous in the isolation key, which we encode
    # into the value prefix for easy parsing here.
    for batch in flat:
        values = [value for _key, value in batch]
        groups = {v[:1] for v in values}
        assert groups == {values[0][:1]}, (
            'wire batch mixed isolation keys: %r' % values)

    # Global order preserved across batches.
    in_order = [value for batch in flat for _k, value in batch]
    assert in_order == [b'a1', b'a2', b'b1', b'a3']

    # The first two A's may share one batch; B and the later A must each be
    # sealed into their own batch (>= 3 batches total, never one).
    assert len(flat) >= 3


def test_isolation_retry_keeps_order_and_boundaries(cross_thread_wakeup):
    """First wire request fails with a retriable error; the retried batch
    must go out ahead of anything later, keys remain unmerged, and every
    Future succeeds exactly once."""
    broker = _make_broker()
    wire_batches = []
    call = {'n': 0}

    def handler(api_key, api_version, correlation_id, request_bytes):
        call['n'] += 1
        wire_batches.append(parse_wire_batches(request_bytes, api_version))
        if call['n'] == 1:
            return _error_response(api_version,
                                   Errors.NotLeaderForPartitionError)
        return _success_response(api_version)

    for _ in range(64):
        broker.respond_fn(ProduceRequest, handler)

    producer = _make_producer(broker, linger_ms=0)
    try:
        # A batch fails, then B/A records sent while it is in flight/retry.
        f_a = producer.send(_TOPIC, value=b'a1', partition=0,
                            isolation_key='A')
        # Block until a1 is done: the first attempt fails but the batch is
        # reenqueued (its future is NOT completed), so this returns only
        # after the retry succeeds.
        f_a.get(timeout=10)
        futures = [f_a]
        for value, key in [(b'b1', 'B'), (b'a2', 'A'), (b'b2', 'B')]:
            futures.append(producer.send(_TOPIC, value=value, partition=0,
                                         isolation_key=key))
        for f in futures:
            assert f.get(timeout=10) is not None
    finally:
        producer.close(timeout=10)

    flat = [batch for request_batches in wire_batches
            for batch in request_batches]
    assert len(flat) >= 2
    for batch in flat:
        prefixes = {value[:1] for _k, value in batch}
        assert len(prefixes) == 1, 'mixed isolation keys in batch: %r' % batch

    in_order = [value for batch in flat for _k, value in batch]
    # a1 was sent twice (attempt + retry), but the committed order must be
    # a1, b1, a2, b2: drop the duplicate failed attempt by tracking the
    # success-offset monotonicity -- here simply assert the distinct tail.
    distinct = []
    for v in in_order:
        if v not in distinct:
            distinct.append(v)
    assert distinct == [b'a1', b'b1', b'a2', b'b2']

    # First wire batch was the A batch; its retry must precede later sends.
    assert flat[0][0][1] == b'a1'
    assert flat[1][0][1] == b'a1', (
        'retry of the failed batch must be the second batch on the wire')


def test_isolation_split_on_message_too_large(cross_thread_wakeup):
    """A multi-record isolated batch that gets MESSAGE_TOO_LARGE is split:
    the split pieces keep the same key, stay ordered, do not merge with a
    later different-key batch, and all original Futures complete."""
    broker = _make_broker()
    wire_batches = []
    call = {'n': 0}

    def handler(api_key, api_version, correlation_id, request_bytes):
        call['n'] += 1
        wire_batches.append(parse_wire_batches(request_bytes, api_version))
        if call['n'] == 1:
            return _error_response(api_version,
                                   Errors.MessageSizeTooLargeError)
        return _success_response(api_version)

    for _ in range(64):
        broker.respond_fn(ProduceRequest, handler)

    producer = _make_producer(broker, linger_ms=0)
    try:
        # Four A records share one batch; the oversized response forces a
        # split. Then a B record must remain in its own homogeneous batch.
        futures = [producer.send(_TOPIC, value=('a%d' % i).encode(),
                                 partition=0, isolation_key='A')
                   for i in range(4)]
        futures.append(producer.send(_TOPIC, value=b'b0', partition=0,
                                     isolation_key='B'))
        results = []
        for f in futures:
            results.append(f.get(timeout=10))
    finally:
        producer.close(timeout=10)

    assert all(r is not None for r in results)

    flat = [batch for request_batches in wire_batches
            for batch in request_batches]
    # First attempt: one A batch of 4. Afterwards: at least two split A
    # batches (2+2), then the standalone B batch.
    assert len(flat) >= 4, flat
    for batch in flat:
        prefixes = {value[:1] for _k, value in batch}
        assert len(prefixes) == 1, 'mixed keys after split: %r' % batch

    # The final (post-split) batches contain A pieces first, then B.
    post = flat[1:]
    a_values = [v for batch in post for _k, v in batch if v[:1] == b'a']
    b_values = [v for batch in post for _k, v in batch if v[:1] == b'b']
    assert sorted(a_values) == [b'a0', b'a1', b'a2', b'a3']
    assert b_values == [b'b0']

    # B must be the very last wire batch (never merged ahead/into A splits).
    assert flat[-1] == [(None, b'b0')]

    # Order within A is preserved across the split.
    assert a_values == [b'a0', b'a1', b'a2', b'a3']


def test_isolation_failed_future_after_exhausted_retries(cross_thread_wakeup):
    """When a retriable error exhausts retries, the isolated batch's Future
    fails -- but later batches are unaffected and still complete."""
    broker = _make_broker()

    def handler(api_key, api_version, correlation_id, request_bytes):
        return _error_response(api_version, Errors.NotLeaderForPartitionError)

    for _ in range(16):
        broker.respond_fn(ProduceRequest, handler)

    producer = _make_producer(broker, linger_ms=0, retries=0)
    try:
        f_a = producer.send(_TOPIC, value=b'a1', partition=0,
                            isolation_key='A')
        with pytest.raises(Errors.NotLeaderForPartitionError):
            f_a.get(timeout=10)
        assert f_a.failed()

        # Switch the broker back to success for the later, separate batch.
        broker._always_responses.clear()
        broker._response_queue.clear()

        def ok(api_key, api_version, correlation_id, request_bytes):
            return _success_response(api_version)
        for _ in range(16):
            broker.respond_fn(ProduceRequest, ok)

        f_b = producer.send(_TOPIC, value=b'b1', partition=0,
                            isolation_key='B')
        assert f_b.get(timeout=10) is not None
    finally:
        producer.close(timeout=10)
