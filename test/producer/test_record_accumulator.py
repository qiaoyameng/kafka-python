# pylint: skip-file

import pytest

import kafka.errors as Errors
from kafka.cluster import ClusterMetadata
from kafka.producer.record_accumulator import RecordAccumulator
from kafka.record.default_records import DefaultRecordBatchBuilder
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

def test_linger(tp, cluster):
    now = 0
    accum = RecordAccumulator(linger_ms=10)
    accum.append(tp, 0, b'key', b'value', [], now=now)
    ready, next_ready_check, _unknown_leaders_exist = accum.ready(cluster, now=now)
    assert len(ready) == 0, 'No partitions should be ready'
    assert next_ready_check == .01 # linger_ms in secs
    now += .01
    ready, _next_ready_check, _unknown_leaders_exist = accum.ready(cluster, now=now)
    assert ready == set([0]), "Our partitions leader should be ready"
    batches = accum.drain(cluster, ready, 0, 2147483647)[0]
    assert len(batches) == 1
    batch = batches[0]
    assert batch.records.is_full()

    parsed = list(batch.records.records())
    assert len(parsed) == 1
    records = list(parsed[0])
    assert len(records) == 1
    assert records[0].key == b'key', 'Keys should match'
    assert records[0].value == b'value', 'Values should match'

def _advance_now_ms(now, ms):
    return now + ms / 1000 + 1/10000 # add extra .1 ms to each advance to avoid rounding issues when converting back to seconds

def _do_expire_batch_single(cluster, tp, delivery_timeout_ms):
    now = 0
    linger_ms = 300
    accum = RecordAccumulator(linger_ms=linger_ms, delivery_timeout_ms=delivery_timeout_ms, request_timeout_ms=(delivery_timeout_ms-linger_ms-100))

    # Make the batches ready due to linger. These batches are not in retry
    for mute in [False, True]:
        accum.append(tp, 0, b'key', b'value', [], now=now)
        ready, next_ready_check, _unknown_leaders_exist = accum.ready(cluster, now=now)
        assert len(ready) == 0, 'No partitions should be ready'
        assert next_ready_check == linger_ms / 1000

        now = _advance_now_ms(now, linger_ms)
        ready, _next_ready_check, _unknown_leaders_exist = accum.ready(cluster, now=now)
        assert ready == set([0]), "Our partitions leader should be ready"

        expired_batches = accum.expired_batches(now=now)
        assert len(expired_batches) == 0, "The batch should not expire when just linger has passed"

        if mute:
            accum.muted.add(tp)
        else:
            try:
                accum.muted.remove(tp)
            except KeyError:
                pass

        # Advance the clock to expire the batch.
        now = _advance_now_ms(now, delivery_timeout_ms - linger_ms)
        expired_batches = accum.expired_batches(now=now)
        assert len(expired_batches) == 1, "The batch may expire when the partition is muted"
        ready, _next_ready_check, _unknown_leaders_exist = accum.ready(cluster, now=now)
        assert len(ready) == 0, "No partitions should be ready."

def test_expired_batch_single(cluster, tp):
    _do_expire_batch_single(cluster, tp, 3200)

def test_expired_batch_single_max_value(cluster, tp):
    _do_expire_batch_single(cluster, tp, 2147483647)

def _expected_num_appends(batch_size):
    size = DefaultRecordBatchBuilder.header_size_in_bytes()
    offset_delta = 0
    while True:
        record_size = DefaultRecordBatchBuilder.size_in_bytes(offset_delta, 0, b'key', b'value', [])
        if size + record_size > batch_size:
            return offset_delta
        offset_delta += 1
        size += record_size

def test_expired_batches(cluster, tp):
    now = 0
    retry_backoff_ms = 100
    linger_ms = 30
    request_timeout_ms = 60
    delivery_timeout_ms = 3200
    batch_size = 1024
    accum = RecordAccumulator(linger_ms=linger_ms, delivery_timeout_ms=delivery_timeout_ms, request_timeout_ms=request_timeout_ms, retry_backoff_ms=retry_backoff_ms, batch_size=batch_size)
    appends = _expected_num_appends(batch_size)

    # Test batches not in retry
    for i in range(appends):
        accum.append(tp, 0, b'key', b'value', [], now=now)
        ready, next_ready_check, _unknown_leaders_exist = accum.ready(cluster, now=now)
        assert len(ready) == 0, 'No partitions should be ready'
        assert next_ready_check == linger_ms / 1000

    # Make the batches ready due to batch full
    accum.append(tp, 0, b'key', b'value', [], now=now)
    ready, _next_ready_check, _unknown_leaders_exist = accum.ready(cluster, now=now)
    assert ready == set([0]), "Our partitions leader should be ready"

    # Advance the clock to expire the batch.
    now = _advance_now_ms(now, delivery_timeout_ms + 1)
    accum.muted.add(tp)
    expired_batches = accum.expired_batches(now=now)
    assert len(expired_batches) == 2, "The batches will be expired no matter if the partition is muted or not"

    accum.muted.remove(tp)
    expired_batches = accum.expired_batches(now=now)
    assert len(expired_batches) == 0, "All batches should have been expired earlier"
    ready, _next_ready_check, _unknown_leaders_exist = accum.ready(cluster, now=now)
    assert len(ready) == 0, "No partitions should be ready."

    # Test batches in retry.
    # Create a retried batch
    accum.append(tp, 0, b'key', b'value', [], now=now)
    now = _advance_now_ms(now, linger_ms)
    ready, _next_ready_check, _unknown_leaders_exist = accum.ready(cluster, now=now)
    assert ready == set([0]), "Our partitions leader should be ready"

    drained = accum.drain(cluster, ready, 2147483647, now=now)
    assert len(drained[0]) == 1, "There should be only one batch."
    now = _advance_now_ms(now, 1000)
    accum.reenqueue(drained[0][0], now=now)

    # test expiration.
    now = _advance_now_ms(now, request_timeout_ms + retry_backoff_ms)
    expired_batches = accum.expired_batches(now=now)
    assert len(expired_batches) == 0, "The batch should not be expired."
    now = _advance_now_ms(now, 1)

    accum.muted.add(tp)
    expired_batches = accum.expired_batches(now=now)
    assert len(expired_batches) == 0, "The batch should not be expired when the partition is muted"

    accum.muted.remove(tp)
    expired_batches = accum.expired_batches(now=now)
    assert len(expired_batches) == 0, "The batch should not be expired when the partition is unmuted"

    now = _advance_now_ms(now, linger_ms)
    ready, _next_ready_check, _unknown_leaders_exist = accum.ready(cluster, now=now)
    assert ready == set([0]), "Our partitions leader should be ready"

    # Advance the clock to expire the batch.
    now = _advance_now_ms(now, delivery_timeout_ms + 1)
    accum.muted.add(tp)
    expired_batches = accum.expired_batches(now=now)
    assert len(expired_batches) == 1, "The batch should not be expired when the partition is muted"


def test_abort_on_new_batch_returns_sentinel(tp):
    """KIP-480 plumbing: with abort_on_new_batch=True and no in-progress
    batch, append must return (None, False, False, True) instead of
    allocating a fresh batch."""
    accum = RecordAccumulator()
    future, batch_full, new_batch, abort = accum.append(
        tp, 0, b'k', b'v', [], now=0, abort_on_new_batch=True)
    assert future is None
    assert batch_full is False
    assert new_batch is False
    assert abort is True
    # And no batch was actually allocated.
    assert tp not in accum._batches or not accum._batches[tp]


def test_abort_batches_handles_in_flight(tp, cluster):
    """abort_batches must fail in-flight (drained but not-yet-acked) batches
    too -- used on fatal-error / force-close paths where the user's pending
    futures must resolve immediately. Drained batches were popleft()'d out
    of _batches[tp]; the .remove() call must not ValueError."""
    accum = RecordAccumulator(linger_ms=0, batch_size=64)

    # In-flight batch.
    inflight_future, *_ = accum.append(tp, 0, b'k1', b'v1', [], now=0)
    ready, _, _ = accum.ready(cluster, now=0)
    drained = accum.drain(cluster, ready, 2147483647, now=0)[0]
    assert len(drained) == 1
    assert drained[0].drained is not None

    # Undrained batch.
    undrained_future, *_ = accum.append(tp, 0, b'k2', b'v2', [], now=0)

    err = Errors.KafkaError('fatal')
    accum.abort_batches(err)

    assert inflight_future.failed()
    assert undrained_future.failed()
    assert isinstance(inflight_future.exception, Errors.KafkaError)
    assert isinstance(undrained_future.exception, Errors.KafkaError)
    assert not accum.has_incomplete


def test_abort_undrained_batches_skips_in_flight(tp, cluster):
    """abort_undrained_batches must not touch batches that have already been
    drained -- drain() popleft()s them out of _batches[tp], so attempting to
    .remove() them raises ValueError; and we want their futures to resolve
    via the broker response, not be cancelled locally."""
    accum = RecordAccumulator(linger_ms=0, batch_size=64)

    # First batch: append, drain (now in-flight), still in _incomplete.
    inflight_future, *_ = accum.append(tp, 0, b'k1', b'v1', [], now=0)
    ready, _, _ = accum.ready(cluster, now=0)
    assert ready == {0}
    drained = accum.drain(cluster, ready, 2147483647, now=0)[0]
    assert len(drained) == 1
    inflight_batch = drained[0]
    assert inflight_batch.drained is not None
    assert tp not in accum._batches or inflight_batch not in accum._batches[tp]

    # Second batch: append, leave undrained.
    undrained_future, *_ = accum.append(tp, 0, b'k2', b'v2', [], now=0)
    assert accum.has_incomplete

    # Must not ValueError on the in-flight batch's missing _batches[tp] entry.
    accum.abort_undrained_batches(Errors.TransactionAbortedError())

    assert undrained_future.failed()
    assert isinstance(undrained_future.exception, Errors.TransactionAbortedError)
    # In-flight batch's future is left alone for the broker response to resolve.
    assert not inflight_future.is_done


def test_abort_on_new_batch_appends_to_existing(tp):
    """If a batch already exists with room, abort_on_new_batch=True still
    succeeds - only the new-batch-allocation path is gated."""
    accum = RecordAccumulator()
    # First append (with abort_on_new_batch=False) creates the batch.
    future1, _, new1, abort1 = accum.append(
        tp, 0, b'k1', b'v1', [], now=0, abort_on_new_batch=False)
    assert future1 is not None
    assert new1 is True
    assert abort1 is False
    # Second append with abort_on_new_batch=True lands in the existing batch.
    future2, _, new2, abort2 = accum.append(
        tp, 0, b'k2', b'v2', [], now=0, abort_on_new_batch=True)
    assert future2 is not None
    assert new2 is False
    assert abort2 is False


# ---------------------------------------------------------------------------
# Batch isolation key
# ---------------------------------------------------------------------------

def _drain_all(accum, cluster, now):
    """Drain every queued batch for tp regardless of linger, one sender
    iteration at a time (drain pops one batch per partition per call)."""
    out = []
    linger = accum.config['linger_ms'] / 1000
    while True:
        ready, _, _ = accum.ready(cluster, now=now)
        if not ready:
            # Advance past linger so the next tail batch becomes ready.
            now += linger + 1 / 1000
            ready, _, _ = accum.ready(cluster, now=now)
            if not ready:
                break
        drained = accum.drain(cluster, ready, 2147483647, now=now)[0]
        if not drained:
            break
        out.extend(drained)
        now += 1 / 1000
    return out


def _parse_batch(batch):
    parsed = list(batch.records.records())
    assert len(parsed) == 1
    return list(parsed[0])


def test_isolation_key_change_seals_current_batch(tp, cluster):
    """Switching isolation_key with linger still pending must seal the
    current batch immediately: both batches are independently drainable and
    no batch mixes keys."""
    now = 0
    accum = RecordAccumulator(linger_ms=10_000, batch_size=10 ** 9)

    f1, full1, new1, abort1 = accum.append(tp, 0, None, b'a', [], now=now, isolation_key='A')
    assert new1 is True and full1 is False
    # Same key continues to fill the open batch.
    f2, full2, new2, _ = accum.append(tp, 0, None, b'b', [], now=now, isolation_key='A')
    assert new2 is False and f2 is not None
    # Different key seals the A batch and starts a B batch, even with a huge
    # linger and plenty of batch_size room.
    f3, full3, new3, _ = accum.append(tp, 0, None, b'c', [], now=now, isolation_key='B')
    assert new3 is True
    assert full3 is True  # sealed A batch sits ahead of the new tail

    dq = accum._batches[tp]
    assert len(dq) == 2
    assert [b.isolation_key for b in dq] == ['A', 'B']
    assert [b.record_count for b in dq] == [2, 1]

    # Head (A) batch is send-ready immediately despite linger=10s; tail (B)
    # alone would still linger.
    ready, _, _ = accum.ready(cluster, now=now)
    assert ready == {0}

    batches = _drain_all(accum, cluster, now)
    assert [b.isolation_key for b in batches] == ['A', 'B']
    values = [[r.value for r in _parse_batch(b)] for b in batches]
    assert values == [[b'a', b'b'], [b'c']]
    # Records keep FIFO send order across the sealed boundary.
    assert [v for group in values for v in group] == [b'a', b'b', b'c']


def test_isolation_key_no_merge_across_intervening_record(tp, cluster):
    """A later record with an earlier isolation key must NOT merge into the
    earlier batch once a different-key record sits in between. Sequence
    A,B,A must come out as three batches in that order."""
    now = 0
    accum = RecordAccumulator(linger_ms=10_000, batch_size=10 ** 9)

    futures = []
    for value, iso in [(b'a1', 'A'), (b'b1', 'B'), (b'a2', 'A'), (b'a3', 'A')]:
        future, _, new_batch, _ = accum.append(
            tp, 0, None, value, [], now=now, isolation_key=iso)
        futures.append((future, new_batch))

    assert [new for _, new in futures] == [True, True, True, False]
    dq = accum._batches[tp]
    assert [b.isolation_key for b in dq] == ['A', 'B', 'A']

    batches = _drain_all(accum, cluster, now)
    assert len(batches) == 3
    assert [[r.value for r in _parse_batch(b)] for b in batches] == [
        [b'a1'], [b'b1'], [b'a2', b'a3']]


def test_isolation_key_none_still_batches(tp, cluster):
    """Records without an isolation key keep the legacy batching behavior:
    everything lands in one None-keyed batch."""
    accum = RecordAccumulator(linger_ms=10_000, batch_size=10 ** 9)
    for v in (b'a', b'b', b'c'):
        accum.append(tp, 0, None, v, [], now=0)
    dq = accum._batches[tp]
    assert len(dq) == 1
    assert dq[0].isolation_key is None
    assert dq[0].record_count == 3


def test_isolation_key_then_none_separated(tp, cluster):
    """An explicit key and None never share a batch, in either direction."""
    accum = RecordAccumulator(linger_ms=10_000, batch_size=10 ** 9)
    accum.append(tp, 0, None, b'a', [], now=0, isolation_key=None)
    accum.append(tp, 0, None, b'b', [], now=0, isolation_key='X')
    accum.append(tp, 0, None, b'c', [], now=0, isolation_key=None)
    keys = [b.isolation_key for b in accum._batches[tp]]
    assert keys == [None, 'X', None]


def test_isolation_key_switch_abort_on_new_batch_stays_on_partition(tp):
    """KIP-480 sticky path: when abort_on_new_batch=True but the only reason
    for a new batch is an isolation switch, append must allocate on the SAME
    partition (no abort/sticky rotation) rather than returning the abort
    sentinel -- isolation never changes partition routing."""
    accum = RecordAccumulator()
    f1, _, new1, abort1 = accum.append(
        tp, 0, None, b'a', [], now=0, abort_on_new_batch=True,
        isolation_key='A')
    # No batch existed at all: standard sticky abort still applies.
    assert abort1 is True and f1 is None

    # Create the A batch for real.
    accum.append(tp, 0, None, b'a', [], now=0, abort_on_new_batch=False,
                 isolation_key='A')

    f2, full2, new2, abort2 = accum.append(
        tp, 0, None, b'b', [], now=0, abort_on_new_batch=True,
        isolation_key='B')
    assert abort2 is False
    assert new2 is True
    assert f2 is not None
    assert [b.isolation_key for b in accum._batches[tp]] == ['A', 'B']


def test_isolation_key_batch_full_does_not_seal_extra_batch(tp):
    """When the tail batch is genuinely full (same isolation key), a normal
    second batch carries that same key -- isolation does not add extra
    batches beyond what batch_size already requires."""
    accum = RecordAccumulator(batch_size=128)
    appends = _expected_num_appends(128)
    for _ in range(appends):
        accum.append(tp, 0, b'key', b'value', [], now=0, isolation_key='A')
    assert len(accum._batches[tp]) == 1
    # One more record rolls the full batch; new batch keeps key A.
    _, _, new_batch, _ = accum.append(
        tp, 0, b'key', b'value', [], now=0, isolation_key='A')
    assert new_batch is True
    assert [b.isolation_key for b in accum._batches[tp]] == ['A', 'A']


def test_isolation_key_bounded_open_batches_per_partition(tp):
    """Rapidly alternating keys seals one batch per switch (the same queue
    growth that full batches produce) but keeps exactly ONE open batch for
    the whole partition -- never one open batch per distinct key."""
    accum = RecordAccumulator(batch_size=10 ** 9, linger_ms=0)
    for i in range(100):
        accum.append(tp, 0, None, str(i).encode(), [], now=0,
                     isolation_key='A' if i % 2 == 0 else 'B')
    dq = accum._batches[tp]
    assert len(dq) == 100
    # Every batch except the tail is "sealed" purely by ordering: the
    # accumulator will only ever append into the tail.
    tail = dq[-1]
    assert tail.isolation_key == 'B'
    # Same key as the tail joins it (no new batch)...
    _, _, new_same, _ = accum.append(
        tp, 0, None, b'b100', [], now=0, isolation_key='B')
    assert new_same is False
    assert len(accum._batches[tp]) == 100
    # ...and a switch adds exactly one more batch, proving there is a single
    # open tail rather than an open batch cached per key.
    _, _, new_switch, _ = accum.append(
        tp, 0, None, b'a100', [], now=0, isolation_key='A')
    assert new_switch is True
    assert len(accum._batches[tp]) == 101


def _multi_record_batch(num_records, isolation_key=None, topic='foo', partition=0):
    from kafka.record.memory_records import MemoryRecordsBuilder
    from kafka.producer.producer_batch import ProducerBatch
    tpart = TopicPartition(topic, partition)
    records = MemoryRecordsBuilder(magic=2, compression_type=0, batch_size=100000)
    batch = ProducerBatch(tpart, records, isolation_key=isolation_key)
    futures = []
    for i in range(num_records):
        futures.append(batch.try_append(0, b'k%d' % i, b'v%d' % i, []))
    batch.records.close()
    return batch, futures


def test_isolation_key_preserved_through_split(tp):
    """split_and_reenqueue must propagate the failed batch's isolation key
    to every sub-batch, so retried splits still cannot mix with other keys."""
    accum = RecordAccumulator()
    batch, _ = _multi_record_batch(num_records=4, isolation_key='S')
    accum._incomplete.add(batch)
    accum.split_and_reenqueue(batch)
    accum.deallocate(batch)
    assert accum._batches[tp], 'split batches should be enqueued'
    for new_batch in accum._batches[tp]:
        assert new_batch.isolation_key == 'S'


def test_isolation_key_preserved_through_reenqueue(tp):
    """A retried batch keeps its isolation key when reenqueued at the head
    of the partition deque."""
    accum = RecordAccumulator()
    future, _, _, _ = accum.append(
        tp, 0, None, b'a', [], now=0, isolation_key='R')
    assert future is not None
    batch = accum._batches[tp][0]
    batch.records.close()
    accum._batches[tp].popleft()
    accum.reenqueue(batch, now=1)
    assert accum._batches[tp][0].isolation_key == 'R'
