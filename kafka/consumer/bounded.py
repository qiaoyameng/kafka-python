"""Finite ("bounded") consumption support for offline reconciliation jobs.

A bounded run reads only the data that already existed when the run started:
the visible end offset of every target partition is captured once, polling
stops as soon as each partition's position reaches that boundary, and
messages appended afterwards can never extend or stall the run. Completion
is derived from positions vs. the snapshot - never from an idle timeout.
"""
import logging

import kafka.errors as Errors
from kafka.structs import TopicPartition
from kafka.util import Timer

log = logging.getLogger(__name__)


class BoundedConsumption:
    """Context manager for a single bounded-consumption run.

    Obtain one from :meth:`KafkaConsumer.bounded_consumption` and use it as a
    context manager; inside the block, :meth:`poll` and ordinary consumer
    iteration return only records below the captured end offsets. Do not
    construct directly.
    """

    def __init__(self, consumer, partitions=None, timeout_ms=None):
        self._consumer = consumer
        self._fetcher = consumer._fetcher
        self._subscription = consumer._subscription
        self._timeout_ms = timeout_ms or consumer.config['default_api_timeout_ms']
        if partitions is not None:
            partitions = list(partitions)
            for tp in partitions:
                if not isinstance(tp, TopicPartition):
                    raise TypeError('partitions must be TopicPartition instances')
                if not self._subscription.is_assigned(tp):
                    raise Errors.IllegalStateError(
                        'Partition %s is not assigned; assign() it before'
                        ' starting bounded consumption' % (tp,))
        self._requested_partitions = partitions
        self.end_offsets = None
        self._entered = False
        self._closed = False

    # -- context manager -------------------------------------------------

    def __enter__(self):
        if self._entered:
            raise Errors.IllegalStateError('BoundedConsumption is not re-entrant')
        self._entered = True

        if self._requested_partitions is None:
            partitions = list(self._subscription.assigned_partitions())
            if not partitions:
                raise Errors.IllegalStateError(
                    'No partitions assigned; assign() partitions (or pass them'
                    ' explicitly) before starting bounded consumption')
            self._requested_partitions = partitions
        partitions = self._requested_partitions

        # Verify existence *explicitly* before snapshotting: a fresh metadata
        # refresh tells "deleted partition" apart from "metadata not loaded".
        self._refresh_partition_metadata(partitions)
        for tp in partitions:
            known = self._consumer._cluster.partitions_for_topic(tp.topic)
            if known is None or tp.partition not in known:
                raise Errors.PartitionUnavailableError(tp)

        # LATEST list-offsets already carries the consumer's isolation_level:
        # read_committed gets the last stable offset (LSO), never the LEO.
        self.end_offsets = self._fetcher.bounded_end_offsets(
            partitions, timeout_ms=self._timeout_ms)

        # Ensure every partition has a fetch position (committed offset or
        # auto_offset_reset) before arming, otherwise no fetch goes out and
        # the run could report completion with records still unread.
        self._ensure_initial_positions(partitions)

        # Validate starting positions against the snapshot: a committed/seeked
        # position already beyond the boundary must surface rather than be
        # silently treated as finished.
        for tp in partitions:
            pos = self._subscription.assignment[tp].position
            end = self.end_offsets[tp]
            if pos.offset > end:
                raise Errors.OffsetBeyondSnapshotError(tp, pos.offset, end)

        self._fetcher.begin_bounded_consumption(self.end_offsets)
        log.info('Bounded consumption started for %s', self.end_offsets)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def close(self):
        if not self._closed:
            self._fetcher.end_bounded_consumption()
            self._closed = True

    # -- run API ---------------------------------------------------------

    def poll(self, timeout_ms=None, max_records=None):
        """Poll records strictly inside the snapshot boundaries.

        Unlike :meth:`KafkaConsumer.poll`, ``timeout_ms`` here bounds only the
        wait for records currently in flight; it never signals completion.

        Returns:
            dict[TopicPartition, list[ConsumerRecord]]: may be empty when
            records were not ready within ``timeout_ms``; check :meth:`done`.
        """
        self._check_active()
        max_records = max_records or self._consumer.config['max_poll_records']
        timer = Timer(timeout_ms if timeout_ms is not None else self._timeout_ms)
        while not self._consumer._closed:
            self._check_partitions_still_exist()
            records = self._consumer._poll_once(
                timer, max_records, update_offsets=True)
            self._check_seek_violations()
            if records:
                return records
            if timer.expired:
                return {}
        return {}

    def __iter__(self):
        self._check_active()
        while not self.done():
            records = self.poll(timeout_ms=self._timeout_ms)
            for partition_records in records.values():
                for record in partition_records:
                    yield record

    def done(self):
        """True once every snapshot partition's position reached its boundary.

        Position-based completion: a quiet broker with no new records does not
        count as done while any partition still has offsets to read.
        """
        self._check_active()
        self._check_partitions_still_exist()
        return all(self._position(tp) >= self.end_offsets[tp]
                   for tp in self._requested_partitions)

    def remaining(self, partition=None):
        """Records still to read: per partition, or total across partitions."""
        self._check_active()
        if partition is not None:
            return max(0, self.end_offsets[partition] - self._position(partition))
        return sum(max(0, self.end_offsets[tp] - self._position(tp))
                   for tp in self._requested_partitions)

    # -- internals -------------------------------------------------------

    def _position(self, tp):
        state = self._subscription.assignment.get(tp)
        if state is None or state.position is None:
            self._ensure_initial_positions([tp])
            state = self._subscription.assignment[tp]
        return state.position.offset

    def _check_active(self):
        if not self._entered:
            raise Errors.IllegalStateError(
                'BoundedConsumption must be used as a context manager')
        if self._closed:
            raise Errors.IllegalStateError('BoundedConsumption has been closed')
        if self._consumer._closed:
            raise Errors.IllegalStateError('KafkaConsumer is closed')

    def _ensure_initial_positions(self, partitions):
        # Drive committed-offset restore and auto-reset until every target
        # partition has a fetch position. Mirrors KafkaConsumer.poll's setup
        # but blocks to completion within the configured timeout.
        timer = Timer(self._timeout_ms)
        while not timer.expired:
            if all(self._subscription.assignment[tp].position is not None
                   for tp in partitions if self._subscription.is_assigned(tp)):
                return
            self._consumer._coordinator.poll(timeout_ms=timer.timeout_ms)
            self._consumer._refresh_committed_offsets(timeout_ms=timer.timeout_ms)
            reset_task = self._fetcher.reset_offsets_if_needed(timeout_ms=timer.timeout_ms)
            if reset_task is not None and not reset_task.is_done:
                self._consumer._net.wait_for(
                    reset_task, timeout_ms=timer.timeout_ms, raise_error=False)
        missing = [tp for tp in partitions
                   if self._subscription.assignment[tp].position is None]
        if missing:
            raise Errors.KafkaTimeoutError(
                'Could not establish fetch positions for %s within %s ms'
                % (missing, self._timeout_ms))

    def _refresh_partition_metadata(self, partitions):
        timer = Timer(self._timeout_ms)
        topics = {tp.topic for tp in partitions}
        for topic in topics:
            if self._consumer._cluster.partitions_for_topic(topic) is None:
                self._consumer.partitions_for_topic(
                    topic, timeout_ms=timer.timeout_ms)

    def _check_partitions_still_exist(self):
        # Explicit "partition disappeared": if metadata no longer lists a
        # snapshot partition that still has records to read, fail loudly.
        for tp in self._requested_partitions:
            state = self._subscription.assignment[tp]
            if state.position is not None and state.position.offset >= self.end_offsets[tp]:
                continue
            known = self._consumer._cluster.partitions_for_topic(tp.topic)
            if known is None or tp.partition not in known:
                # Force one authoritative refresh before declaring it gone,
                # so a transiently stale cache cannot trigger a false positive.
                self._refresh_partition_metadata([tp])
                known = self._consumer._cluster.partitions_for_topic(tp.topic)
                if known is None or tp.partition not in known:
                    raise Errors.PartitionUnavailableError(tp)

    def _check_seek_violations(self):
        for tp in self._requested_partitions:
            state = self._subscription.assignment.get(tp)
            if state is None or state.position is None:
                continue
            if state.position.offset > self.end_offsets[tp]:
                raise Errors.OffsetBeyondSnapshotError(
                    tp, state.position.offset, self.end_offsets[tp])
