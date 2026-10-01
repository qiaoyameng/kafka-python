# pylint: skip-file
"""Unit tests for the explicit-ack commit watermark manager.

These cover the ordering invariants directly; the wire-level rebalance /
commit behavior is exercised in test_ack_manager_mock_broker.py.
"""

from collections import namedtuple

import pytest

import kafka.errors as Errors
from kafka.consumer.ack import (
    AckHandle, AckManager, StaleAckHandleError,
)
from kafka.structs import OffsetAndMetadata, TopicPartition


Rec = namedtuple('Rec', ['topic', 'partition', 'offset'])

TP = TopicPartition('t', 0)
TP1 = TopicPartition('t', 1)


def rec(offset, tp=TP):
    return Rec(tp.topic, tp.partition, offset)


class FakeConsumer:
    """Minimal stand-in for the commit sink side of KafkaConsumer."""

    def __init__(self, enable_auto_commit=False, raises=None):
        self.config = {'enable_auto_commit': enable_auto_commit}
        self.commits = []
        self._raises = raises

    def commit(self, offsets, timeout_ms=None):
        if self._raises is not None:
            raise self._raises
        self.commits.append(dict(offsets))


@pytest.fixture
def manager():
    return AckManager()


# --------------------------------------------------------------------------- #
# Out-of-order completion watermark
# --------------------------------------------------------------------------- #

class TestWatermark:

    def test_empty_partition_has_no_offset(self, manager):
        assert manager.completed_offset(TP) is None
        assert manager.committable() == {}

    def test_in_order_completion_advances_prefix(self, manager):
        handles = [manager.track(rec(i)) for i in range(10, 13)]
        assert manager.completed_offset(TP) == 10

        handles[0].ack()
        assert manager.completed_offset(TP) == 11
        handles[1].ack()
        assert manager.completed_offset(TP) == 12
        handles[2].ack()
        assert manager.completed_offset(TP) == 13

    def test_out_of_order_does_not_skip_in_flight(self, manager):
        h10, h11, h12 = (manager.track(rec(i)) for i in (10, 11, 12))

        # Completing 12 first must not move the watermark past 10.
        h12.ack()
        assert manager.completed_offset(TP) == 10
        assert manager.pending_offsets(TP) == [10, 11]
        assert manager.holes(TP) == [10, 11]
        assert manager.committable() == {}

        # 11 next: still blocked by 10.
        h11.ack()
        assert manager.completed_offset(TP) == 10

        # Finally 10: the whole prefix collapses at once.
        h10.ack()
        assert manager.completed_offset(TP) == 13
        assert manager.holes(TP) == []
        assert manager.pending_offsets(TP) == []

    def test_gap_remains_hole_until_ack_or_abandon(self, manager):
        h10, h11, h12 = (manager.track(rec(i)) for i in (10, 11, 12))
        h10.ack()
        h12.ack()
        # 11 failed and is simply never resolved: the prefix stays at 11.
        assert manager.completed_offset(TP) == 11
        assert manager.holes(TP) == [11]

        # Explicitly abandoning 11 advances the watermark over it.
        abandoned = []
        manager._on_abandon = lambda tp, off, h: abandoned.append((tp, off))
        h11.abandon()
        assert manager.completed_offset(TP) == 13
        assert abandoned == [(TP, 11)]

    def test_abandon_is_explicit_only_no_implicit_skip(self, manager):
        # A "failed" record that nobody ack()s or abandon()s forever blocks.
        h10, h11, h12 = (manager.track(rec(i)) for i in (10, 11, 12))
        h10.ack()
        h12.ack()
        for _ in range(100):  # no timeout / retry magic
            assert manager.completed_offset(TP) == 11

    def test_ack_is_idempotent(self, manager):
        h = manager.track(rec(10))
        h.ack()
        h.ack()  # worker retry
        assert manager.completed_offset(TP) == 11

    def test_abandon_is_idempotent_and_fires_callback_once(self, manager):
        fired = []
        manager._on_abandon = lambda tp, off, h: fired.append(off)
        h = manager.track(rec(10))
        h.abandon()
        h.abandon()
        assert fired == [10]
        assert manager.completed_offset(TP) == 11

    def test_per_partition_watermarks_are_independent(self, manager):
        h0 = manager.track(rec(10, TP))
        h1 = manager.track(rec(20, TP1))
        h0.ack()
        # TP1 still pending, but its hole must not block TP0.
        due = manager.committable()
        assert due == {TP: OffsetAndMetadata(11, '', -1)}
        assert manager.completed_offset(TP1) == 20


# --------------------------------------------------------------------------- #
# Commit sink
# --------------------------------------------------------------------------- #

class TestCommit:

    def test_commit_due_only_advances_contiguous_prefix(self):
        consumer = FakeConsumer()
        manager = AckManager(consumer=consumer)
        h10, h11, h12 = (manager.track(rec(i)) for i in (10, 11, 12))
        h12.ack()
        assert manager.commit_due() == {}
        assert consumer.commits == []

        h10.ack()
        committed = manager.commit_due()
        assert committed == {TP: OffsetAndMetadata(11, '', -1)}  # blocked by 11
        h11.ack()
        # 12 was acked first; once 11 lands the prefix jumps to 13.
        committed = manager.commit_due()
        assert committed == {TP: OffsetAndMetadata(13, '', -1)}
        assert consumer.commits == [
            {TP: OffsetAndMetadata(11, '', -1)},
            {TP: OffsetAndMetadata(13, '', -1)},
        ]

        # Nothing new -> no redundant commit.
        assert manager.commit_due() == {}
        assert len(consumer.commits) == 2

    def test_commit_due_marks_committed_even_when_more_due_later(self):
        consumer = FakeConsumer()
        manager = AckManager(consumer=consumer)
        handles = [manager.track(rec(i)) for i in range(0, 5)]
        handles[0].ack()
        handles[1].ack()
        manager.commit_due()
        assert consumer.commits[-1] == {TP: OffsetAndMetadata(2, '', -1)}
        handles[2].ack()
        handles[3].ack()
        handles[4].ack()
        manager.commit_due()
        assert consumer.commits[-1] == {TP: OffsetAndMetadata(5, '', -1)}

    def test_custom_commit_fn(self):
        sink = []
        manager = AckManager(commit_fn=sink.append)
        manager.track(rec(0)).ack()
        manager.commit_due()
        assert sink == [{TP: OffsetAndMetadata(1, '', -1)}]

    def test_commit_failure_keeps_watermark_pending(self):
        consumer = FakeConsumer(raises=Errors.KafkaConnectionError('boom'))
        manager = AckManager(consumer=consumer)
        manager.track(rec(0)).ack()
        with pytest.raises(Errors.KafkaConnectionError):
            manager.commit_due()
        assert manager.committed_offset(TP) == 0
        # Retried after the broker comes back.
        consumer._raises = None
        assert manager.commit_due() == {TP: OffsetAndMetadata(1, '', -1)}

    def test_commit_due_without_sink_raises(self, manager):
        manager.track(rec(0)).ack()
        with pytest.raises(StaleAckHandleError):
            manager.commit_due()


# --------------------------------------------------------------------------- #
# Handle invalidation: seek / revoke / reassign / close
# --------------------------------------------------------------------------- #

class TestHandleInvalidation:

    def test_revocation_commits_prefix_then_invalidates_handles(self):
        consumer = FakeConsumer()
        manager = AckManager(consumer=consumer)
        h10, h11, h12 = (manager.track(rec(i)) for i in (10, 11, 12))
        h10.ack()
        h11.ack()
        # 12 still in flight.

        manager.on_partitions_revoked({TP})

        # Only the confirmed prefix through 12 was committed.
        assert consumer.commits == [{TP: OffsetAndMetadata(12, '', -1)}]
        # Old handles are stale; a late ack must be rejected.
        assert not h12.valid
        with pytest.raises(StaleAckHandleError):
            h12.ack()
        # Partition state was dropped.
        assert TP not in manager.partitions()

    def test_revocation_with_hole_commits_prefix_only(self):
        consumer = FakeConsumer()
        manager = AckManager(consumer=consumer)
        h10, h11, h12 = (manager.track(rec(i)) for i in (10, 11, 12))
        h10.ack()
        h12.ack()  # 11 missing
        manager.on_partitions_revoked({TP})
        assert consumer.commits == [{TP: OffsetAndMetadata(11, '', -1)}]

    def test_revocation_without_prefix_does_not_commit(self):
        consumer = FakeConsumer()
        manager = AckManager(consumer=consumer)
        h10 = manager.track(rec(10))
        h11 = manager.track(rec(11))
        h11.ack()  # 10 still in flight
        manager.on_partitions_revoked({TP})
        assert consumer.commits == []
        with pytest.raises(StaleAckHandleError):
            h10.ack()

    def test_revocation_commit_failure_still_invalidates(self):
        consumer = FakeConsumer(raises=Errors.KafkaConnectionError('down'))
        manager = AckManager(consumer=consumer)
        h = manager.track(rec(0))
        h.ack()
        # Coordinator path treats listener exceptions as fatal; but the
        # manager must have invalidated regardless.
        with pytest.raises(Errors.KafkaConnectionError):
            manager.on_partitions_revoked({TP})
        assert not h.valid
        assert TP not in manager.partitions()

    def test_reassignment_invalidates_old_generation_handles(self):
        manager = AckManager()
        old = [manager.track(rec(i)) for i in range(0, 3)]
        manager.on_partitions_revoked({TP})
        manager.on_partitions_assigned({TP})
        for handle in old:
            assert not handle.valid
            with pytest.raises(StaleAckHandleError):
                handle.ack()

        # New generation starts clean.
        new = [manager.track(rec(i)) for i in range(5, 8)]
        new[0].ack()
        assert manager.completed_offset(TP) == 6
        new[1].ack()
        new[2].ack()
        assert manager.completed_offset(TP) == 8

    def test_assigned_partition_also_invalidates_stale_handles(self):
        manager = AckManager()
        h = manager.track(rec(0))
        # Partition re-assigned to us without a tracked revoke (e.g.
        # coordinator churn): the old handle must still die.
        manager.on_partitions_assigned({TP})
        assert not h.valid
        with pytest.raises(StaleAckHandleError):
            h.ack()

    def test_seek_invalidates_handles_and_seeds_new_base(self, manager):
        h0, h1 = manager.track(rec(0)), manager.track(rec(1))
        manager.seek(TP, 500)
        for handle in (h0, h1):
            assert not handle.valid
            with pytest.raises(StaleAckHandleError):
                handle.ack()
        # Seeded at the seek position: nothing committed ahead of data.
        assert manager.completed_offset(TP) == 500
        assert manager.committed_offset(TP) == 500
        h500 = manager.track(rec(500))
        h500.ack()
        assert manager.completed_offset(TP) == 501

    def test_on_partitions_lost_does_not_commit(self):
        consumer = FakeConsumer()
        manager = AckManager(consumer=consumer)
        h = manager.track(rec(0))
        h.ack()
        manager.on_partitions_lost({TP})
        assert consumer.commits == []
        assert not h.valid

    def test_close_commits_only_confirmed_prefix(self):
        consumer = FakeConsumer()
        manager = AckManager(consumer=consumer)
        h10, h11, h12 = (manager.track(rec(i)) for i in (10, 11, 12))
        h10.ack()
        h11.ack()
        # 12 remains in flight at shutdown.
        manager.close()
        assert consumer.commits == [{TP: OffsetAndMetadata(12, '', -1)}]
        assert not h12.valid
        with pytest.raises(StaleAckHandleError):
            h12.ack()
        with pytest.raises(Errors.IllegalStateError):
            manager.track(rec(13))
        # Idempotent.
        manager.close()

    def test_close_commit_failure_still_invalidates(self):
        consumer = FakeConsumer(raises=Errors.KafkaConnectionError('down'))
        manager = AckManager(consumer=consumer)
        h = manager.track(rec(0))
        h.ack()
        manager.close()  # must not raise
        assert not h.valid


# --------------------------------------------------------------------------- #
# Handle object
# --------------------------------------------------------------------------- #

def test_handle_attributes():
    manager = AckManager()
    h = manager.track(rec(7))
    assert isinstance(h, AckHandle)
    assert h.partition == TP
    assert h.offset == 7
    assert h.valid
    assert 'offset=7' in repr(h)


def test_track_batch_shape():
    manager = AckManager()
    batch = manager.track_batch({TP: [rec(0), rec(1)], TP1: [rec(0)]})
    assert len(batch[TP]) == 2
    assert len(batch[TP1]) == 1
    record, handle = batch[TP][0]
    assert record.offset == 0 and handle.offset == 0


# --------------------------------------------------------------------------- #
# AckingConsumer wrapper
# --------------------------------------------------------------------------- #

class FakeKafkaConsumer:
    """Fake of the KafkaConsumer surface used by AckingConsumer."""

    def __init__(self, enable_auto_commit=False):
        self.config = {'enable_auto_commit': enable_auto_commit}
        self._coordinator = None
        self.committed = []
        self.subscriptions = []
        self.seeks = []
        self.closed_with = None
        self._next = {}

    def commit(self, offsets, timeout_ms=None):
        self.committed.append(dict(offsets))

    def subscribe(self, topics=(), pattern=None, listener=None):
        self.subscriptions.append((topics, pattern, listener))
        self.listener = listener

    def assign(self, partitions):
        self.assigned = list(partitions)

    def unsubscribe(self):
        self.unsubscribed = True

    def poll(self, timeout_ms=0, max_records=None):
        return dict(self._next)

    def seek(self, partition, offset):
        self.seeks.append((partition, offset))

    def assignment(self):
        return getattr(self, 'assigned', [])

    def position(self, partition, timeout_ms=None):
        return None

    def close(self, autocommit=True, timeout_ms=None):
        self.closed_with = (autocommit, timeout_ms)


class TestAckingConsumer:

    def test_requires_auto_commit_disabled(self):
        from kafka.consumer.ack import AckingConsumer
        with pytest.raises(Errors.KafkaConfigurationError):
            AckingConsumer(FakeKafkaConsumer(enable_auto_commit=True))

    def test_poll_returns_record_handle_pairs(self):
        from kafka.consumer.ack import AckingConsumer
        consumer = FakeKafkaConsumer()
        acking = AckingConsumer(consumer)
        consumer._next = {TP: [rec(0, TP), rec(1, TP)]}
        pairs = acking.poll()
        assert [r.offset for r, _ in pairs] == [0, 1]
        pairs[0][1].ack()
        assert acking.completed_offset(TP) == 1

    def test_poll_batched_preserves_partition_grouping(self):
        from kafka.consumer.ack import AckingConsumer
        consumer = FakeKafkaConsumer()
        acking = AckingConsumer(consumer)
        consumer._next = {TP: [rec(0, TP)], TP1: [rec(0, TP1)]}
        batch = acking.poll_batched()
        assert set(batch) == {TP, TP1}

    def test_subscribe_installs_async_listener_chaining_user(self):
        from kafka.consumer.ack import (
            AckingConsumer, AsyncAckRebalanceListener,
        )
        consumer = FakeKafkaConsumer()
        acking = AckingConsumer(consumer)

        user_events = []

        class UserListener:
            def on_partitions_revoked(self, revoked):
                user_events.append(('revoked', set(revoked)))
            def on_partitions_assigned(self, assigned):
                user_events.append(('assigned', set(assigned)))

        acking.subscribe([TP.topic], listener=UserListener())
        _, _, listener = consumer.subscriptions[0]
        assert isinstance(listener, AsyncAckRebalanceListener)

        # Revocation path through the installed async listener. The fake
        # consumer commit is synchronous, so the coroutine runs straight
        # through under asyncio.
        import asyncio
        asyncio.run(listener.on_partitions_revoked({TP}))
        assert ('revoked', {TP}) in user_events

    def test_seek_invalidates_handles(self):
        from kafka.consumer.ack import AckingConsumer
        consumer = FakeKafkaConsumer()
        acking = AckingConsumer(consumer)
        handle = acking.manager.track(rec(0))
        acking.seek(TP, 100)
        assert consumer.seeks == [(TP, 100)]
        assert not handle.valid
        assert acking.completed_offset(TP) == 100

    def test_close_commits_prefix_then_closes_consumer_without_autocommit(self):
        from kafka.consumer.ack import AckingConsumer
        consumer = FakeKafkaConsumer()
        acking = AckingConsumer(consumer)
        handle = acking.manager.track(rec(0))
        handle.ack()
        acking.close()
        assert consumer.committed == [{TP: OffsetAndMetadata(1, '', -1)}]
        assert consumer.closed_with[0] is False


# --------------------------------------------------------------------------- #
# Real parallel workers
# --------------------------------------------------------------------------- #

def test_parallel_workers_out_of_order_commit_is_consistent():
    """Many worker threads resolve records in arbitrary completion order;
    the committed watermark must always equal a prefix (no skipped hole)."""
    import random
    import threading

    consumer = FakeConsumer()
    manager = AckManager(consumer=consumer)
    n_partitions = 4
    n_records = 200
    tps = [TopicPartition('t', p) for p in range(n_partitions)]

    handles = []
    for tp in tps:
        for i in range(n_records):
            handles.append((tp, i, manager.track(rec(i, tp))))

    def worker(batch, seed):
        rng = random.Random(seed)
        rng.shuffle(batch)
        for _tp, _i, handle in batch:
            handle.ack()

    threads = []
    chunks = [handles[i::8] for i in range(8)]
    for idx, chunk in enumerate(chunks):
        t = threading.Thread(target=worker, args=(chunk, idx))
        threads.append(t)
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Every record acked -> full watermarks.
    for tp in tps:
        assert manager.pending_offsets(tp) == []
        assert manager.holes(tp) == []
        assert manager.completed_offset(tp) == n_records

    committed = manager.commit_due()
    assert committed == {tp: OffsetAndMetadata(n_records, '', -1)
                         for tp in tps}


def test_concurrent_rebalance_invalidates_inflight_handles():
    """A revoke landing while workers are still acking makes late acks fail
    stale rather than corrupting the post-rebalance watermark."""
    import threading

    manager = AckManager()
    handles = [manager.track(rec(i)) for i in range(50)]
    stale_acks, ok_acks = [], []
    lock = threading.Lock()
    started = threading.Event()

    def slow_worker():
        started.set()
        for idx, handle in enumerate(handles):
            # Hold the worker on the very first ack until the main thread
            # has revoked the partition, deterministically forcing the
            # stale path for the remaining handles.
            if idx == 0:
                started.wait()
                revoke_landed.wait()
            try:
                handle.ack()
                with lock:
                    ok_acks.append(handle.offset)
            except StaleAckHandleError:
                with lock:
                    stale_acks.append(handle.offset)

    revoke_landed = threading.Event()
    t = threading.Thread(target=slow_worker)
    t.start()
    started.wait()
    manager.on_partitions_revoked({TP})
    revoke_landed.set()
    t.join()

    # Every handle either landed before invalidation or was rejected;
    # nothing silently applied to a dropped state.
    assert len(ok_acks) + len(stale_acks) == 50
    assert stale_acks  # the revoke did cut off some late acks
    assert TP not in manager.partitions()
