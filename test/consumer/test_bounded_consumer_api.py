# pylint: skip-file
"""KafkaConsumer-level bounded-consumption API checks (mocked I/O).

Verifies wiring: ``consumer.bounded_consumption(...)`` returns a
BoundedConsumption, and ``seek`` beyond an active snapshot boundary raises
explicitly rather than silently moving the position outside the snapshot.
"""
import pytest
from unittest.mock import PropertyMock

import kafka.errors as Errors
from kafka import KafkaConsumer
from kafka.consumer.bounded import BoundedConsumption
from kafka.structs import TopicPartition


@pytest.fixture
def consumer(mocker):
    consumer = KafkaConsumer(api_version=(0, 10, 0))
    mocker.patch.object(consumer, 'close', autospec=True)
    mocker.patch.object(consumer, '_refresh_committed_offsets')
    mocker.patch.object(consumer._cluster, 'partitions_for_topic',
                        return_value={0})
    mocker.patch.object(consumer._fetcher, 'bounded_end_offsets',
                        return_value={TopicPartition('t', 0): 5})
    mocker.patch.object(consumer._coordinator, 'poll', return_value=True)
    mocker.patch.object(consumer._fetcher, 'reset_offsets_if_needed',
                        return_value=None)
    # Ensure the partition already has a fetch position inside the snapshot.
    from kafka.structs import OffsetAndMetadata
    consumer._subscription.assign_from_user([TopicPartition('t', 0)])
    consumer._subscription.seek(TopicPartition('t', 0),
                                OffsetAndMetadata(0, '', -1))
    yield consumer
    consumer.close()


def test_bounded_consumption_returns_context_manager(consumer):
    run = consumer.bounded_consumption(
        partitions=[TopicPartition('t', 0)], timeout_ms=1000)
    assert isinstance(run, BoundedConsumption)
    with run as entered:
        assert entered is run
        assert run.end_offsets == {TopicPartition('t', 0): 5}
        assert consumer._fetcher.bounded_end_offsets_snapshot == \
            {TopicPartition('t', 0): 5}
    assert consumer._fetcher.bounded_end_offsets_snapshot is None


def test_seek_within_snapshot_allowed(consumer):
    tp = TopicPartition('t', 0)
    with consumer.bounded_consumption(partitions=[tp], timeout_ms=1000):
        consumer.seek(tp, 4)  # < end offset 5
        assert consumer._subscription.position(tp).offset == 4


def test_seek_beyond_snapshot_boundary_rejected(consumer):
    tp = TopicPartition('t', 0)
    with consumer.bounded_consumption(partitions=[tp], timeout_ms=1000):
        with pytest.raises(Errors.OffsetBeyondSnapshotError) as exc:
            consumer.seek(tp, 6)  # > end offset 5
        assert exc.value.partition == tp
        assert exc.value.requested_offset == 6
        assert exc.value.snapshot_end_offset == 5
        # Position is unchanged - the illegal seek did not take effect.
        assert consumer._subscription.position(tp).offset == 0


def test_seek_boundary_not_enforced_after_run_ends(consumer):
    tp = TopicPartition('t', 0)
    with consumer.bounded_consumption(partitions=[tp], timeout_ms=1000):
        pass
    # After the run, normal seeks are unrestricted.
    consumer.seek(tp, 100)
    assert consumer._subscription.position(tp).offset == 100
