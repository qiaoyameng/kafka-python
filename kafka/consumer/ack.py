"""Explicit acknowledgment with per-partition commit watermarks.

When business logic processes fetched records in parallel (thread / process
pools, async tasks), records can finish *out of order*: offset 12 may be
processed before offset 11 that is still in flight. A naïve "commit on
completion" strategy would commit offset 13 and silently skip offset 11 if
the consumer then crashed or rebalanced.

This module solves that with an *explicit* ack API:

* Every polled record is handed to the business as an
  :class:`AckHandle` pair via :meth:`AckManager.track`. Workers call
  :meth:`AckHandle.ack` on success or :meth:`AckHandle.abandon` to give up
  a record deliberately. Until one of those happens the record is a *hole*.
* Only the *contiguous completed prefix* is committable. Finishing offset
  12 while 11 is still in flight advances nothing.
* Handles carry a per-partition generation. ``seek``, partition revocation
  and reassignment all invalidate the outstanding handles (they raise
  :class:`StaleAckHandleError`), so late callbacks from an old generation
  can never corrupt a new watermark.
* Failures are never skipped implicitly: a failed record either remains a
  hole (blocking the prefix) or is released via an explicit ``abandon``.
* On close / revoke, only the acknowledged prefix is committed; in-flight
  records are redelivered after the rebalance / restart.

:class:`AsyncAckRebalanceListener` plugs straight into
``KafkaConsumer.subscribe(..., listener=...)`` (it awaits the
revoke-time offset commit on the IO loop, so the event loop is never
re-entered); :class:`AckManager` can also be used standalone with any
offset sink. :class:`AckingConsumer` is a thin convenience wrapper that
wires the manager to a consumer for the common case.
"""

import inspect
import logging
import threading

from kafka.consumer.subscription_state import AsyncConsumerRebalanceListener
import kafka.errors as Errors
from kafka.errors import KafkaError
from kafka.structs import OffsetAndMetadata, TopicPartition
from kafka.util import synchronized

log = logging.getLogger(__name__)


class StaleAckHandleError(KafkaError):
    """Raised when an :class:`AckHandle` is used after its partition
    generation was invalidated by a seek, partition revocation, or
    reassignment.

    A stale handle must never move the new generation's watermark. Callers
    that pool worker results should treat this as expected (e.g. drop the
    late result) rather than retry the ack against the new generation.
    """
    pass


class _PartitionState:
    """Ack bookkeeping for one (generation of one) partition.

    ``base`` is the offset of the oldest record handed out in this
    generation (the position before the first tracked record). The
    committable offset is ``base + len(contiguous completed prefix)``.
    """

    __slots__ = ('base', 'completed', 'pending', 'abandoned', 'committed')

    def __init__(self, base, committed):
        self.base = base
        self.completed = set()
        self.pending = set()
        self.abandoned = set()
        # The offset we have already durably committed, <= base initially.
        self.committed = committed

    def deliver(self, offset):
        # base is fixed at the first record of the generation; duplicate
        # delivery (re-delivery / retried poll) simply re-enters the set.
        self.pending.add(offset)

    def resolve(self, offset, abandoned):
        if offset in self.pending:
            self.pending.discard(offset)
            self.completed.add(offset)
            if abandoned:
                self.abandoned.add(offset)
            return True
        if offset in self.completed:
            # Idempotent duplicate resolution (e.g. worker retry); do not
            # re-fire the abandoned callback.
            return False
        raise KeyError(offset)

    def completed_prefix_end(self):
        """First offset above the contiguous run of completed records
        starting at ``base`` (which equals the committable offset)."""
        end = self.base
        while end in self.completed:
            end += 1
        return end

    def holes(self):
        """Sorted not-completed offsets strictly below the highest tracked
        offset -- i.e. records blocking prefix advancement."""
        if not self.pending:
            return []
        highest = max(self.completed | self.pending)
        return sorted(off for off in range(self.base, highest + 1)
                      if off not in self.completed)


class AckHandle:
    """An explicit-acknowledgment handle for a single delivered record.

    Returned by :meth:`AckManager.track` (and
    :meth:`AckingConsumer.poll`) alongside the record. The handle is bound
    to one partition generation: after a seek, revocation, reassignment,
    or manager close it becomes *stale* and any further :meth:`ack` /
    :meth:`abandon` raises :class:`StaleAckHandleError`.

    Handles are safe to share across worker threads; all manager state is
    lock-protected.
    """

    __slots__ = ('_manager', '_tp', '_offset', '_epoch', '_abandoned')

    def __init__(self, manager, tp, offset, epoch):
        self._manager = manager
        self._tp = tp
        self._offset = offset
        self._epoch = epoch
        self._abandoned = False

    @property
    def partition(self):
        return self._tp

    @property
    def offset(self):
        return self._offset

    @property
    def valid(self):
        """True if this handle still belongs to a live partition generation."""
        return self._manager._handle_epoch(self._tp) == self._epoch

    def ack(self):
        """Mark the record successfully processed.

        The commit watermark advances only if this record completes the
        contiguous prefix for its partition.

        Raises:
            StaleAckHandleError: if the handle's generation was invalidated.
        """
        self._manager._resolve(self._tp, self._offset, self._epoch,
                               abandoned=False, handle=self)

    def abandon(self):
        """Explicitly give up this record without re-processing it.

        An abandoned record counts toward the contiguous prefix (so it no
        longer blocks the watermark) but is reported through the manager's
        ``on_abandon`` callback. Unlike a silently lost failure, this is a
        deliberate decision -- there is no implicit skip path.

        Raises:
            StaleAckHandleError: if the handle's generation was invalidated.
        """
        if self._abandoned:
            # Idempotent: abandon() twice from e.g. a failure handler is fine.
            return
        self._manager._resolve(self._tp, self._offset, self._epoch,
                               abandoned=True, handle=self)
        # Only latch after the manager accepted it (a stale handle raises).
        self._abandoned = True

    def __repr__(self):
        return ('AckHandle(topic=%r, partition=%r, offset=%r, valid=%r)'
                % (self._tp.topic, self._tp.partition, self._offset,
                   self.valid))


class AckManager:  # sync hooks; AsyncAckRebalanceListener adapts for IO
    """Tracks explicit acks and exposes per-partition commit watermarks.

    The manager guarantees that the offset offered for commit for a
    partition is the next offset after the *longest contiguous run* of
    resolved (acked or explicitly abandoned) records starting from the
    generation's base. Out-of-order completion therefore never skips an
    in-flight record.

    Two ways to drive the actual offset commit:

    1. Wire to a ``KafkaConsumer`` via :class:`AckingConsumer` (or attach
       :class:`AsyncAckRebalanceListener` yourself). On revoke the
       confirmed prefix is committed on the IO loop (awaiting an async
       offset commit so the event loop is never re-entered); on
       :meth:`close` / :meth:`commit_due` the due prefixes are committed
       synchronously. Auto-commit must be disabled, otherwise the consumer
       would commit fetched (not acked) offsets itself.
    2. Standalone. Pass ``commit_fn`` (called as
       ``commit_fn({TopicPartition: OffsetAndMetadata})``) and use the
       manager with any poll source / offset sink.

    Arguments:
        consumer: Optional KafkaConsumer used for ``position()`` and sync
            ``commit()`` when ``commit_fn`` is not supplied.
        commit_fn: Optional callable taking an offsets dict and committing
            it. Takes precedence over ``consumer.commit``.
        on_abandon: Optional callable invoked as
            ``on_abandon(tp, offset, handle)`` when a record is explicitly
            abandoned (e.g. dead-letter queue, metric).
    """

    def __init__(self, consumer=None, commit_fn=None, on_abandon=None):
        self._consumer = consumer
        if commit_fn is not None:
            self._commit_fn = commit_fn
        elif consumer is not None:
            self._commit_fn = consumer.commit
        else:
            self._commit_fn = None
        self._on_abandon = on_abandon

        self._states = {}       # TopicPartition -> _PartitionState
        self._epochs = {}       # TopicPartition -> monotonically increasing int
        self._closed = False
        # Reentrant so commit paths can call read helpers while locked.
        self._lock = threading.RLock()

    # ------------------------------------------------------------------ #
    # Handle lifecycle
    # ------------------------------------------------------------------ #

    @synchronized
    def _handle_epoch(self, tp):
        return self._epochs.get(tp)

    @synchronized
    def track(self, record):
        """Register one delivered record and return its :class:`AckHandle`.

        Call this for every record returned by ``poll`` before dispatching
        it to workers. The manager is agnostic to record type -- anything
        exposing ``topic``, ``partition`` and ``offset`` attributes works
        (``ConsumerRecord`` does).

        Raises:
            IllegalStateError: if the manager is closed.
        """
        if self._closed:
            raise Errors.IllegalStateError('AckManager is closed')
        tp = TopicPartition(record.topic, record.partition)
        offset = int(record.offset)

        state = self._states.get(tp)
        if state is None:
            # Records of one generation are handed out in fetch order, so
            # the first tracked record is the generation base.
            state = _PartitionState(base=offset, committed=offset)
            self._states[tp] = state
            self._epochs.setdefault(tp, 0)

        state.deliver(offset)
        return AckHandle(self, tp, offset, self._epochs[tp])

    def track_batch(self, records_by_partition):
        """Convenience: :meth:`track` every record from a
        ``poll()``-shaped ``{tp: [records]}`` dict and return the same
        shape with handles: ``{tp: [(record, handle), ...]}``."""
        result = {}
        for tp, records in records_by_partition.items():
            pairs = []
            for record in records:
                pairs.append((record, self.track(record)))
            result[tp] = pairs
        return result

    def _resolve(self, tp, offset, epoch, abandoned, handle):
        # Split locked mutation from the (user-supplied) abandon callback
        # so callbacks can safely call back into the manager.
        with self._lock:
            if self._epochs.get(tp) != epoch:
                raise StaleAckHandleError(
                    'Ack handle for %s offset %d is stale (generation'
                    ' invalidated by seek, revocation, or reassignment)'
                    % (tp, offset))
            state = self._states[tp]
            try:
                is_new = state.resolve(offset, abandoned)
            except KeyError:
                is_new = False
        if abandoned and is_new and self._on_abandon is not None:
            self._on_abandon(tp, offset, handle)

    # ------------------------------------------------------------------ #
    # Watermark queries
    # ------------------------------------------------------------------ #

    @synchronized
    def completed_offset(self, tp):
        """The committable offset for ``tp`` (next after the contiguous
        completed prefix), or ``None`` if the partition is not tracked."""
        state = self._states.get(tp)
        if state is None:
            return None
        return state.completed_prefix_end()

    @synchronized
    def committed_offset(self, tp):
        """Last offset durably committed for ``tp`` (or ``None``)."""
        state = self._states.get(tp)
        return None if state is None else state.committed

    @synchronized
    def pending_offsets(self, tp):
        """Sorted offsets delivered but not yet acked/abandoned."""
        state = self._states.get(tp)
        return [] if state is None else sorted(state.pending)

    @synchronized
    def holes(self, tp):
        """Sorted unresolved offsets that are blocking prefix advancement."""
        state = self._states.get(tp)
        return [] if state is None else state.holes()

    @synchronized
    def partitions(self):
        """Set of partitions currently tracked."""
        return set(self._states)

    def committable(self):
        """Return ``{tp: OffsetAndMetadata}`` for partitions whose
        contiguous-prefix watermark is ahead of the last committed offset.
        """
        with self._lock:
            items = list(self._states.items())
        offsets = {}
        for tp, state in items:
            due = state.completed_prefix_end()
            if due > state.committed:
                offsets[tp] = OffsetAndMetadata(due, '', -1)
        return offsets

    def commit_due(self, timeout_ms=None):
        """Commit every partition's advanced watermark.

        Only contiguous completed prefixes are committed; partitions blocked
        on a hole are skipped. Records the committed watermarks on success.

        Arguments:
            timeout_ms (numeric, optional): Forwarded to a consumer-backed
                sync commit. Ignored for custom ``commit_fn`` sinks.

        Returns:
            The offsets dict that was committed (empty if nothing was due).

        Raises:
            Errors.KafkaError: if no commit sink is configured or the
            (synchronous) commit fails.
        """
        offsets = self.committable()
        if not offsets:
            return {}
        if self._commit_fn is None:
            raise StaleAckHandleError(
                'No commit sink configured: pass a consumer or commit_fn')
        self._invoke_commit(offsets, timeout_ms)
        with self._lock:
            for tp, oam in offsets.items():
                self._states[tp].committed = oam.offset
        return offsets

    def _invoke_commit(self, offsets, timeout_ms):
        # consumer.commit(offsets, timeout_ms=...); custom commit_fn takes
        # only the offsets mapping.
        if self._consumer is not None and self._commit_fn == self._consumer.commit:
            self._commit_fn(offsets, timeout_ms=timeout_ms)
        else:
            self._commit_fn(offsets)

    # ------------------------------------------------------------------ #
    # Rebalance / close integration
    # ------------------------------------------------------------------ #

    def _due_offsets_for(self, partitions):
        """Build the due-offset mapping for a subset of partitions."""
        with self._lock:
            selected = [(tp, self._states[tp]) for tp in partitions
                        if tp in self._states]
        offsets = {}
        for tp, state in selected:
            due = state.completed_prefix_end()
            if due > state.committed:
                offsets[tp] = OffsetAndMetadata(due, '', -1)
        return offsets

    def _record_committed(self, offsets):
        with self._lock:
            for tp, oam in offsets.items():
                if tp in self._states:
                    self._states[tp].committed = oam.offset

    async def _awaitable_commit(self, offsets):
        """Commit ``offsets`` returning an awaitable.

        With a real KafkaConsumer we commit on the IO loop via
        ``commit_offsets_async`` and await its future, so the rebalance
        listener never re-enters the event loop (a blocking ``commit()``
        inside a selector-driven listener would raise
        ``Recursive access to net.poll``). A standalone ``commit_fn`` is
        called directly (and awaited if it is itself a coroutine).
        """
        if (self._consumer is not None and self._commit_fn == self._consumer.commit
                and offsets):
            coordinator = getattr(self._consumer, '_coordinator', None)
            net = getattr(coordinator, '_net', None)
            if coordinator is not None and net is not None:
                future = coordinator.commit_offsets_async(offsets)
                await net.await_for(future, timeout_ms=None, raise_error=True)
                return
        result = self._commit_fn(offsets)
        if inspect.isawaitable(result):
            await result

    async def _commit_partitions_async(self, partitions):
        """Commit the due prefixes of ``partitions`` only (used on revoke).

        Never commits offsets for partitions we no longer own. Returns the
        committed mapping; raises if the commit fails.
        """
        offsets = self._due_offsets_for(set(partitions))
        if not offsets:
            return {}
        if self._commit_fn is None:
            log.debug('AckManager: no commit sink; skipping commit of %s',
                      offsets)
            return {}
        await self._awaitable_commit(offsets)
        self._record_committed(offsets)
        return offsets

    def _commit_partitions(self, partitions):
        """Blocking variant used by standalone / non-IO-loop callers
        (notably :meth:`close`)."""
        offsets = self._due_offsets_for(set(partitions))
        if not offsets:
            return {}
        if self._commit_fn is None:
            log.debug('AckManager: no commit sink; skipping commit of %s',
                      offsets)
            return {}
        self._invoke_commit(offsets, None)
        self._record_committed(offsets)
        return offsets

    def _invalidate(self, partitions):
        """Bump generations and drop state so every outstanding handle for
        ``partitions`` reports stale."""
        with self._lock:
            for tp in partitions:
                self._epochs[tp] = self._epochs.get(tp, 0) + 1
                self._states.pop(tp, None)

    def on_partitions_revoked(self, revoked):
        """Commit the confirmed prefixes, then invalidate old handles.

        Only the contiguous acknowledged/abandoned prefix is committed;
        records still in flight (holes) are intentionally left for
        redelivery after the rebalance.

        If the final commit fails, the exception is propagated (the
        coordinator surfaces listener failures to ``poll``), but the
        outstanding handles are invalidated regardless in a ``finally``.
        """
        revoked = set(revoked)
        log.info('AckManager: partitions revoked %s; committing confirmed'
                 ' prefixes before invalidation', revoked)
        try:
            self._commit_partitions(revoked)
        finally:
            self._invalidate(revoked)

    def on_partitions_assigned(self, assigned):
        """Start a fresh generation for each newly assigned partition.

        Any handles left over from a previous ownership of the same
        partition are invalidated so they cannot move the new watermark.
        """
        assigned = set(assigned)
        with self._lock:
            for tp in assigned:
                self._epochs[tp] = self._epochs.get(tp, 0) + 1
                self._states.pop(tp, None)

    def on_partitions_lost(self, lost):
        """Forced eviction: we cannot commit, so invalidate without
        committing. In-flight and uncommitted records will be redelivered
        to the new owner."""
        log.warning('AckManager: partitions lost %s without commit;'
                    ' invalidating handles', set(lost))
        self._invalidate(set(lost))

    # ------------------------------------------------------------------ #
    # Seek / reset
    # ------------------------------------------------------------------ #

    @synchronized
    def seek(self, tp, offset):
        """Align the tracker with a consumer ``seek`` to ``offset``.

        The old generation is discarded (all outstanding handles go stale)
        and a new empty generation starts at ``offset``. Records below the
        new position are never committed by the old watermark.
        """
        self._epochs[tp] = self._epochs.get(tp, 0) + 1
        self._states.pop(tp, None)
        # Eagerly seed the new generation so completed_offset() reflects
        # the seek position even before the first record is tracked.
        state = _PartitionState(base=offset, committed=offset)
        self._states[tp] = state

    # ------------------------------------------------------------------ #
    # Shutdown
    # ------------------------------------------------------------------ #

    def close(self, commit=True):
        """Commit confirmed prefixes for all tracked partitions, then
        invalidate every handle and mark the manager closed.

        Only the contiguous acknowledged/abandoned prefix is committed;
        holes remain uncommitted so they are redelivered on restart.

        Arguments:
            commit (bool): Set False to skip the final commit (e.g. the
                consumer is already known to have lost the group).
        """
        if self._closed:
            return
        self._closed = True
        with self._lock:
            partitions = list(self._states)
        if commit:
            try:
                self._commit_partitions(partitions)
            except Exception:
                # In-flight records are redelivered after restart; do not
                # block shutdown. Invalidation below still runs.
                log.exception('AckManager.close: final prefix commit failed;'
                              ' in-flight records will be redelivered')
        self._invalidate(partitions)


class AsyncAckRebalanceListener(AsyncConsumerRebalanceListener):
    """Event-loop-safe rebalance hooks for :class:`AckManager`.

    The revoke-time commit is awaited *on* the IO loop (via
    ``commit_offsets_async``) rather than performed as a blocking call,
    which would re-enter the selector's poll.

    A user-supplied listener is chained after the manager's hooks. The
    user listener may be a sync ``ConsumerRebalanceListener`` or an async
    ``AsyncConsumerRebalanceListener``; sync methods are called inline.
    """

    def __init__(self, manager, user_listener=None):
        self._manager = manager
        self._user = user_listener

    @staticmethod
    async def _maybe_await(result):
        if inspect.isawaitable(result):
            await result

    async def on_partitions_revoked(self, revoked):
        revoked = set(revoked)
        # Commit confirmed prefixes while we are still the recognised
        # owner, then invalidate; finally run user cleanup.
        try:
            await self._manager._commit_partitions_async(revoked)
        finally:
            self._manager._invalidate(revoked)
        if self._user is not None:
            await self._maybe_await(self._user.on_partitions_revoked(revoked))

    async def on_partitions_assigned(self, assigned):
        self._manager.on_partitions_assigned(set(assigned))
        if self._user is not None:
            await self._maybe_await(self._user.on_partitions_assigned(set(assigned)))

    async def on_partitions_lost(self, lost):
        self._manager.on_partitions_lost(set(lost))
        if self._user is not None:
            await self._maybe_await(self._user.on_partitions_lost(set(lost)))


class AckingConsumer:
    """KafkaConsumer wrapper for parallel processing with explicit acks.

    Typical usage::

        consumer = KafkaConsumer('topic', group_id='g',
                                 enable_auto_commit=False)
        acking = AckingConsumer(consumer)
        acking.subscribe(['topic'])

        while True:
            for record, handle in acking.poll(timeout_ms=1000):
                pool.submit(work, record, handle)   # worker: handle.ack()
            acking.commit_due()                      # advance watermarks
        acking.close()

    Completion order is irrelevant: a record at offset 12 that finishes
    before offset 11 never advances the committed watermark past 11.

    Arguments:
        consumer: A ``KafkaConsumer``. ``enable_auto_commit`` must be False
            (auto-commit would commit fetched offsets regardless of acks).
        on_abandon: Optional callable passed through to :class:`AckManager`.
    """

    def __init__(self, consumer, on_abandon=None):
        if consumer.config.get('enable_auto_commit'):
            raise Errors.KafkaConfigurationError(
                'AckingConsumer requires enable_auto_commit=False so that'
                ' fetched-but-unacked offsets cannot be committed')
        self._consumer = consumer
        self.manager = AckManager(consumer=consumer, on_abandon=on_abandon)

    # ------------------------------------------------------------------ #
    # Subscription / assignment (install the rebalance listener)
    # ------------------------------------------------------------------ #

    def subscribe(self, topics=(), pattern=None, listener=None):
        """Subscribe and install the ack rebalance listener.

        Any user ``listener`` (sync or async) is chained after the
        manager's hooks.
        """
        chained = AsyncAckRebalanceListener(self.manager, listener)
        return self._consumer.subscribe(topics=topics, pattern=pattern,
                                        listener=chained)

    def assign(self, partitions):
        """Manual assignment: register fresh generations for the partitions.

        Manual assignments never trigger a rebalance callback, so register
        them explicitly here.
        """
        ret = self._consumer.assign(partitions)
        self.manager.on_partitions_assigned(partitions)
        return ret

    def unsubscribe(self):
        # The coordinator handles its own final auto-commit (disabled
        # here); we only drop ack state so outstanding handles go stale.
        # Use the non-committing invalidation path: this can run right
        # after coordinator teardown, when a sync commit would be invalid.
        partitions = self.manager.partitions()
        ret = self._consumer.unsubscribe()
        self.manager._invalidate(partitions)
        return ret

    # ------------------------------------------------------------------ #
    # Polling with handles
    # ------------------------------------------------------------------ #

    def poll(self, timeout_ms=0, max_records=None):
        """Poll and return a flat list of ``(record, AckHandle)`` pairs.

        Every returned record is registered with the ack manager before it
        reaches the caller, so dispatching records straight to workers is
        safe -- nothing can be completed untracked.
        """
        records_by_partition = self._consumer.poll(
            timeout_ms=timeout_ms, max_records=max_records)
        pairs = []
        for records in records_by_partition.values():
            for record in records:
                pairs.append((record, self.manager.track(record)))
        return pairs

    def poll_batched(self, timeout_ms=0, max_records=None):
        """Like :meth:`poll` but preserves partition grouping:
        ``{TopicPartition: [(record, AckHandle), ...]}``."""
        records_by_partition = self._consumer.poll(
            timeout_ms=timeout_ms, max_records=max_records)
        return self.manager.track_batch(records_by_partition)

    # ------------------------------------------------------------------ #
    # Offset control
    # ------------------------------------------------------------------ #

    def seek(self, partition, offset):
        """Seek the consumer and invalidate all handles for the partition."""
        self._consumer.seek(partition, offset)
        self.manager.seek(partition, offset)

    def commit_due(self, timeout_ms=None):
        """Commit contiguous confirmed prefixes that are due."""
        return self.manager.commit_due(timeout_ms=timeout_ms)

    def completed_offset(self, tp):
        return self.manager.completed_offset(tp)

    def holes(self, tp):
        return self.manager.holes(tp)

    def pending_offsets(self, tp):
        return self.manager.pending_offsets(tp)

    # ------------------------------------------------------------------ #
    # Pass-through / shutdown
    # ------------------------------------------------------------------ #

    def assignment(self):
        return self._consumer.assignment()

    def position(self, partition, timeout_ms=None):
        return self._consumer.position(partition, timeout_ms=timeout_ms)

    def close(self, timeout_ms=None):
        """Commit confirmed prefixes, invalidate handles, then close the
        underlying consumer with its own auto-commit disabled."""
        self.manager.close(commit=True)
        return self._consumer.close(autocommit=False, timeout_ms=timeout_ms)

    @property
    def config(self):
        return self._consumer.config
