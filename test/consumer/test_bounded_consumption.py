# pylint: skip-file
"""Unit tests for the high-level BoundedConsumption controller.

Wire-level snapshot/clipping behavior is covered in
``test_bounded_mock_broker.py``; these tests drive BoundedConsumption
against a lightweight fake consumer/fetcher to verify completion
semantics (position-based, never idle-timeout-based), partition
disappearance, and snapshot boundaries.
"""
import time
import pytest
from unittest.mock import MagicMock

import kafka.errors as Errors
from kafka.consumer.bounded import BoundedConsumption
from kafka.consumer.fetcher import ConsumerRecord
from kafka.structs import OffsetAndMetadata, TopicPartition


TOPIC = 'foobar'
TP0 = TopicPartition(TOPIC, 0)
TP1 = TopicPartition(TOPIC, 1)


class FakeState:
    def __init__(self, offset):
        self.position = OffsetAndMetadata(offset, '', -1)


class FakeSubscription:
    def __init__(self, offsets):
        self.assignment = {tp: FakeState(off) for tp, off in offsets.items()}

    def assigned_partitions(self):
        return set(self.assignment)

    def is_assigned(self, tp):
        return tp in self.assignment

    def seek(self, tp, offset):
        self.assignment[tp].position = OffsetAndMetadata(offset, '', -1)


class FakeFetcher:
    def __init__(self, snapshot):
        self._snapshot = dict(snapshot)
        self._end_offsets = None
        self.began = 0
        self.ended = 0
        self._subs = None

    def bounded_end_offsets(self, partitions, timeout_ms=None):
        return {tp: self._snapshot[tp] for tp in partitions}

    def begin_bounded_consumption(self, end_offsets):
        self.began += 1
        self._end_offsets = dict(end_offsets)

    def end_bounded_consumption(self):
        self.ended += 1
        self._end_offsets = None

    @property
    def bounded_end_offsets_snapshot(self):
        return None if self._end_offsets is None else dict(self._end_offsets)

    def bounded_partitions_finished(self, partitions=None):
        if self._end_offsets is None:
            return set()
        return {tp for tp in self._end_offsets
                if self._subs.assignment[tp].position.offset >= self._end_offsets[tp]}

    def bounded_remaining(self, tp):
        return self._end_offsets[tp] - self._subs.assignment[tp].position.offset

    def reset_offsets_if_needed(self, timeout_ms=None):
        return None


def _record(tp, offset):
    return ConsumerRecord(
        topic=tp.topic, partition=tp.partition, leader_epoch=-1,
        offset=offset, timestamp=0, timestamp_type=0, key=None,
        value=b'x', headers=[], checksum=None,
        serialized_key_size=-1, serialized_value_size=1,
        serialized_header_size=0)


class FakeConsumer:
    def __init__(self, snapshot, batches=None, known_partitions=(0, 1)):
        self.config = {'default_api_timeout_ms': 5000, 'max_poll_records': 500}
        self._closed = False
        self._subscription = FakeSubscription({tp: 0 for tp in snapshot})
        self._fetcher = FakeFetcher(snapshot)
        self._fetcher._subs = self._subscription
        self._coordinator = MagicMock()
        self._coordinator.poll.return_value = True
        self._net = MagicMock()
        self._known_partitions = None if known_partitions is None else set(known_partitions)
        self.poll_calls = 0
        self._batches = list(batches or [])
        self._cluster = MagicMock()
        self._cluster.partitions_for_topic.side_effect = self._partitions_for_topic

    def _partitions_for_topic(self, topic):
        return set(self._known_partitions) if self._known_partitions is not None else None

    def partitions_for_topic(self, topic, timeout_ms=None):
        return self._partitions_for_topic(topic) or set()

    def _refresh_committed_offsets(self, timeout_ms=None):
        return None

    def _poll_once(self, timer, max_records, update_offsets=True):
        self.poll_calls += 1
        if not self._batches:
            return {}
        batch = self._batches.pop(0)
        if not batch:
            # Simulate the broker waiting out fetch_max_wait_ms with no data.
            time.sleep(0.005)
            return {}
        for tp, records in batch.items():
            if records:
                self._subscription.assignment[tp].position = OffsetAndMetadata(
                    records[-1].offset + 1, '', -1)
        return batch


def _make(snapshot, batches=None, known=(0, 1)):
    return FakeConsumer(snapshot, batches=batches, known_partitions=known)


class TestBoundedLifecycle:
    def test_snapshot_captured_once_and_context_disarms(self):
        consumer = _make({TP0: 5})
        with BoundedConsumption(consumer, partitions=[TP0], timeout_ms=1000) as run:
            assert run.end_offsets == {TP0: 5}
            assert consumer._fetcher.began == 1
        assert consumer._fetcher.ended == 1
        assert consumer._fetcher._end_offsets is None

    def test_iterates_exactly_to_snapshot_despite_later_appends(self):
        # Boundary is 3. A trailing batch contains messages appended after the
        # snapshot; done() at position 3 must stop before they are returned.
        batches = [
            {TP0: [_record(TP0, 0), _record(TP0, 1)]},
            {TP0: [_record(TP0, 2)]},
            {TP0: [_record(TP0, 3), _record(TP0, 4)]},
        ]
        consumer = _make({TP0: 3}, batches=batches)
        with BoundedConsumption(consumer, partitions=[TP0], timeout_ms=1000) as run:
            offsets = [r.offset for r in run]
        assert offsets == [0, 1, 2]

    def test_per_partition_boundaries_tracked_independently(self):
        batches = [
            {TP0: [_record(TP0, 0)], TP1: [_record(TP1, 0)]},
            {TP0: [_record(TP0, 1)]},
        ]
        consumer = _make({TP0: 2, TP1: 1}, batches=batches)
        with BoundedConsumption(consumer, timeout_ms=1000) as run:
            assert run.remaining() == 3
            first = run.poll()
            second = run.poll()
            assert run.done()
            assert run.remaining() == 0
        seen = sorted((r.partition, r.offset)
                      for batch in (first, second)
                      for records in batch.values() for r in records)
        assert seen == [(0, 0), (0, 1), (1, 0)]

    def test_empty_snapshot_completes_immediately(self):
        consumer = _make({TP0: 0})
        with BoundedConsumption(consumer, partitions=[TP0], timeout_ms=1000) as run:
            assert run.done()
            assert run.remaining(TP0) == 0
            assert list(run) == []
        assert consumer.poll_calls == 0

    def test_idle_poll_returns_empty_but_not_done(self):
        # Boundary is 2; broker is idle for two polls, then delivers both.
        batches = [{}, {}, {TP0: [_record(TP0, 0), _record(TP0, 1)]}]
        consumer = _make({TP0: 2}, batches=batches)
        with BoundedConsumption(consumer, partitions=[TP0], timeout_ms=100) as run:
            assert run.poll(timeout_ms=1) == {}
            assert not run.done()
            assert run.remaining(TP0) == 2
            assert run.poll(timeout_ms=1) == {}
            assert not run.done()
            records = run.poll(timeout_ms=1)
            assert sorted(r.offset for r in records[TP0]) == [0, 1]
            assert run.done()

    def test_partition_disappearing_mid_run_raises(self):
        batches = [{TP0: [_record(TP0, 0)]}]  # position -> 1, end is 5
        consumer = _make({TP0: 5}, batches=batches)
        with BoundedConsumption(consumer, partitions=[TP0], timeout_ms=100) as run:
            run.poll(timeout_ms=1)
            consumer._known_partitions = None  # partition disappears
            with pytest.raises(Errors.PartitionUnavailableError):
                run.done()

    def test_partition_missing_at_entry_raises(self):
        consumer = _make({TP0: 5}, known=None)
        with pytest.raises(Errors.PartitionUnavailableError):
            with BoundedConsumption(consumer, partitions=[TP0], timeout_ms=100):
                pass

    def test_starting_position_beyond_snapshot_raises(self):
        consumer = _make({TP0: 3})
        consumer._subscription.assignment[TP0].position = OffsetAndMetadata(4, '', -1)
        with pytest.raises(Errors.OffsetBeyondSnapshotError):
            with BoundedConsumption(consumer, partitions=[TP0], timeout_ms=100):
                pass

    def test_unassigned_partition_rejected(self):
        consumer = _make({TP0: 1})
        other = TopicPartition(TOPIC, 9)
        with pytest.raises(Errors.IllegalStateError):
            BoundedConsumption(consumer, partitions=[other], timeout_ms=100)

    def test_usage_without_context_manager_raises(self):
        consumer = _make({TP0: 1})
        run = BoundedConsumption(consumer, partitions=[TP0], timeout_ms=100)
        with pytest.raises(Errors.IllegalStateError):
            run.done()

    def test_no_assignment_at_entry_raises(self):
        consumer = FakeConsumer({}, known_partitions=set())
        with pytest.raises(Errors.IllegalStateError):
            with BoundedConsumption(consumer, timeout_ms=100):
                pass
