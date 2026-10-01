"""Explicit, out-of-order-safe message acknowledgment for KafkaConsumer.

When a business processes fetched records in parallel, completion order no
longer matches offset order. Committing the fetch position would silently
skip records that are still in flight. This module provides an explicit
acknowledgment interface instead:

- The consumer hands out an :class:`Acknowledgment` handle per record
  (``KafkaConsumer.acknowledge(record)``).
- Worker threads resolve handles with ``ack()`` (processed),
  ``fail()`` (leave a gap -- the offset stays uncommitted so it can be
  redelivered), or ``abandon()`` (explicitly skip this offset).
- The committable watermark for each partition only advances over a
  *contiguous* run of resolved offsets: acknowledging offset 12 never
  skips offset 11 while it is still pending.
- Handles are bound to an epoch. ``seek()``, partition revocation, and
  reassignment invalidate all outstanding handles for the affected
  partitions; resolving a stale handle raises ``IllegalStateError``.

``AckTracker`` is thread-safe: handles are meant to be resolved from
worker threads while the consumer thread keeps polling.
"""

import itertools
import logging
import threading

import kafka.errors as Errors
from kafka.structs import OffsetAndMetadata

log = logging.getLogger(__name__)


class Acknowledgment:
    """Handle confirming processing of a single consumed record.

    Obtained from ``KafkaConsumer.acknowledge(record)``. Resolve it with
    :meth:`ack`, :meth:`fail`, or :meth:`abandon`; a failed handle may
    still be abandoned later (explicitly giving up on the gap). Handles
    become invalid when the partition is seeked, revoked, or reassigned
    -- resolving a stale handle raises ``Errors.IllegalStateError``.
    """

    PENDING = 'pending'
    ACKED = 'acked'
    FAILED = 'failed'
    ABANDONED = 'abandoned'

    def __init__(self, tracker, topic_partition, offset, epoch):
        self._tracker = tracker
        self._topic_partition = topic_partition
        self._offset = offset
        self._epoch = epoch
        self._state = self.PENDING

    @property
    def topic_partition(self):
        return self._topic_partition

    @property
    def offset(self):
        return self._offset

    @property
    def state(self):
        """One of 'pending', 'acked', 'failed', 'abandoned'."""
        return self._state

    @property
    def valid(self):
        """True while this handle can still be resolved.

        False once the handle has been resolved, or after the partition
        was seeked / revoked / reassigned (stale epoch).
        """
        return self._tracker._is_valid(self)

    def ack(self):
        """Mark the record as successfully processed."""
        self._resolve(self.ACKED)

    def fail(self):
        """Mark processing as failed, leaving a gap in the watermark.

        The offset stays unresolved: the partition's commit watermark
        will not advance past it, so the record remains eligible for
        redelivery after a restart or rebalance. If the record should
        never be retried, use :meth:`abandon` instead.
        """
        self._resolve(self.FAILED)

    def abandon(self):
        """Explicitly give up on the record, allowing the watermark past it.

        Unlike :meth:`fail`, this resolves the offset so the commit
        watermark may advance over it. Use for poison messages that
        must be skipped deliberately -- offsets are never skipped
        implicitly. May be called on a pending handle or later on a
        failed one, once the business gives up retrying it.
        """
        self._resolve(self.ABANDONED)

    def _resolve(self, outcome):
        self._tracker._resolve(self, outcome)

    def __repr__(self):
        return 'Acknowledgment(%s, offset=%d, state=%s)' % (
            self._topic_partition, self._offset, self._state)


class _PartitionAcks:
    __slots__ = ('epoch', 'base', 'resolved', 'pending')

    def __init__(self, epoch, base):
        self.epoch = epoch
        self.base = base          # lowest offset tracked in this epoch
        self.resolved = set()     # acked or abandoned offsets
        self.pending = 0          # unresolved handle count

    def watermark(self):
        """Next offset to commit: end of the contiguous resolved prefix."""
        offset = self.base
        while offset in self.resolved:
            offset += 1
        return offset


class AckTracker:
    """Tracks explicit acknowledgments per partition and computes the
    contiguous committable watermark for each.

    Also acts as an internal ``SubscriptionState`` listener: seeks and
    dropped (revoked / reassigned) partitions invalidate the partition's
    outstanding handles by bumping its epoch.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._partitions = {}  # TopicPartition -> _PartitionAcks
        self._epochs = itertools.count(1)
        self._active = False

    @property
    def active(self):
        """True once any acknowledgment handle has been created.

        KafkaConsumer uses this to switch its commit paths from
        consumed positions to acknowledged watermarks.
        """
        return self._active

    def begin(self, topic_partition, offset):
        """Register a record as in-flight and return its handle."""
        with self._lock:
            self._active = True
            state = self._partitions.get(topic_partition)
            if state is None:
                state = _PartitionAcks(next(self._epochs), offset)
                self._partitions[topic_partition] = state
            elif offset < state.base:
                state.base = offset
            state.pending += 1
            return Acknowledgment(self, topic_partition, offset, state.epoch)

    def invalidate(self, topic_partition):
        """Drop all acknowledgment state for a partition.

        Outstanding handles for the partition become stale: resolving
        them raises IllegalStateError.
        """
        with self._lock:
            dropped = self._partitions.pop(topic_partition, None)
        if dropped is not None and dropped.pending:
            log.debug("Invalidated %d pending acknowledgments for %s",
                      dropped.pending, topic_partition)

    # -- SubscriptionState listener hooks (internal) ----------------------

    def on_seek(self, topic_partition):
        self.invalidate(topic_partition)

    def on_partitions_dropped(self, partitions):
        for tp in partitions:
            self.invalidate(tp)

    # -- handle resolution (called by Acknowledgment) ---------------------

    def _is_valid(self, handle):
        with self._lock:
            if handle._state in (Acknowledgment.ACKED, Acknowledgment.ABANDONED):
                return False
            state = self._partitions.get(handle._topic_partition)
            return state is not None and state.epoch == handle._epoch

    def _resolve(self, handle, outcome):
        with self._lock:
            # PENDING -> any outcome; FAILED -> ABANDONED (explicitly
            # giving up on a gap). Everything else is a usage error.
            allowed = (
                handle._state == Acknowledgment.PENDING or
                (handle._state == Acknowledgment.FAILED and
                 outcome == Acknowledgment.ABANDONED))
            if not allowed:
                raise Errors.IllegalStateError(
                    'Acknowledgment for %s offset %d already resolved (%s)' % (
                        handle._topic_partition, handle._offset, handle._state))
            state = self._partitions.get(handle._topic_partition)
            if state is None or state.epoch != handle._epoch:
                raise Errors.IllegalStateError(
                    'Stale acknowledgment for %s offset %d: the partition was'
                    ' seeked, revoked, or reassigned since the handle was'
                    ' created' % (handle._topic_partition, handle._offset))
            handle._state = outcome
            state.pending -= 1
            if outcome in (Acknowledgment.ACKED, Acknowledgment.ABANDONED):
                state.resolved.add(handle._offset)

    # -- commit watermarks -------------------------------------------------

    def acknowledged_offsets(self):
        """Return {TopicPartition: OffsetAndMetadata} committable now.

        Each entry is the end of the partition's contiguous resolved
        prefix (i.e. the next offset to consume). Partitions without at
        least one resolved offset are omitted -- nothing may be committed
        for them yet.
        """
        with self._lock:
            offsets = {}
            for tp, state in self._partitions.items():
                watermark = state.watermark()
                if watermark > state.base:
                    offsets[tp] = OffsetAndMetadata(watermark, '')
            return offsets

    def pending_count(self, topic_partition=None):
        """Number of unresolved handles, optionally for one partition."""
        with self._lock:
            if topic_partition is not None:
                state = self._partitions.get(topic_partition)
                return state.pending if state is not None else 0
            return sum(state.pending for state in self._partitions.values())
