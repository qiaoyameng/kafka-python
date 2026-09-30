import time
from types import SimpleNamespace

import pytest

from kafka import TopicPartition
from kafka.consumer.subscription_state import SubscriptionState, TopicPartitionState
from kafka.structs import OffsetAndMetadata


class FakeClock:
    """Deterministic monotonic clock for pause-token expiry tests."""

    def __init__(self, start=1000.0):
        self.now = start

    def monotonic(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


@pytest.fixture
def fake_clock(mocker):
    clock = FakeClock()
    mocker.patch('kafka.consumer.subscription_state.time',
                 SimpleNamespace(monotonic=clock.monotonic))
    return clock


def test_type_error():
    s = SubscriptionState()
    with pytest.raises(TypeError):
        s.subscribe(topics='foo')

    s.subscribe(topics=['foo'])


def test_change_subscription():
    s = SubscriptionState()
    s.subscribe(topics=['foo'])
    assert s.subscription == set(['foo'])
    s.change_subscription(['bar'])
    assert s.subscription == set(['bar'])


def test_group_subscribe():
    s = SubscriptionState()
    s.subscribe(topics=['foo'])
    assert s.subscription == set(['foo'])
    s.group_subscribe(['bar'])
    assert s.subscription == set(['foo'])
    assert s._group_subscription == set(['foo', 'bar'])

    s.reset_group_subscription()
    assert s.subscription == set(['foo'])
    assert s._group_subscription == set(['foo'])


def test_change_subscription_after_assignment():
    s = SubscriptionState()
    s.subscribe(topics=['foo'])
    s.assign_from_subscribed([TopicPartition('foo', 0), TopicPartition('foo', 1)])
    # Changing subscription retains existing assignment until next rebalance
    s.change_subscription(['bar'])
    assert set(s.assignment.keys()) == set([TopicPartition('foo', 0), TopicPartition('foo', 1)])


# ---------------------------------------------------------------------------
# assign_from_subscribed: state-preserving (Java-faithful) assignment update
# ---------------------------------------------------------------------------


class TestAssignFromSubscribed:
    """``assign_from_subscribed`` preserves ``TopicPartitionState`` for
    partitions present in both the old and new assignment. New partitions
    get fresh state; revoked partitions are dropped. This is what
    makes KIP-429 cooperative rebalancing efficient (and what makes
    EAGER rebalances avoid wiping state for partitions that didn't
    move)."""

    def _subscribed(self):
        s = SubscriptionState()
        s.subscribe(topics=['foo'])
        return s

    def test_initial_assignment(self):
        """First-call behaviour (no prior assignment to preserve)."""
        s = self._subscribed()
        with pytest.raises(ValueError):
            s.assign_from_subscribed([TopicPartition('bar', 0)])

        s.assign_from_subscribed([TopicPartition('foo', 0), TopicPartition('foo', 1)])
        assert set(s.assignment.keys()) == set([
            TopicPartition('foo', 0), TopicPartition('foo', 1)])
        assert all(isinstance(tps, TopicPartitionState)
                   for tps in s.assignment.values())
        assert all(not tps.has_valid_position for tps in s.assignment.values())

    def test_preserves_position_on_kept_partition(self):
        s = self._subscribed()
        s.assign_from_subscribed([TopicPartition('foo', 0), TopicPartition('foo', 1)])
        # Seed state on partition 0 that should survive the rebalance.
        s.assignment[TopicPartition('foo', 0)].seek(
            OffsetAndMetadata(offset=100, metadata='', leader_epoch=-1))

        # Rebalance: keep partition 0, drop partition 1, add partition 2.
        s.assign_from_subscribed([
            TopicPartition('foo', 0), TopicPartition('foo', 2)])

        # Kept partition retains its position.
        assert s.assignment[TopicPartition('foo', 0)].position.offset == 100
        # New partition has fresh (no-position) state.
        assert not s.assignment[TopicPartition('foo', 2)].has_valid_position
        # Revoked partition is gone.
        assert TopicPartition('foo', 1) not in s.assignment

    def test_preserves_paused_state_on_kept_partition(self):
        s = self._subscribed()
        s.assign_from_subscribed([TopicPartition('foo', 0)])
        s.pause(TopicPartition('foo', 0))
        assert s.is_paused(TopicPartition('foo', 0))

        s.assign_from_subscribed([
            TopicPartition('foo', 0), TopicPartition('foo', 1)])
        # Pause survived.
        assert s.is_paused(TopicPartition('foo', 0))
        # New partition isn't paused by default.
        assert not s.is_paused(TopicPartition('foo', 1))

    def test_mark_pending_revocation_blocks_fetches(self):
        """KIP-429: a partition flagged for pending revocation must
        report not-fetchable so the fetcher skips it while a revoke
        listener is running."""
        s = self._subscribed()
        s.assign_from_subscribed([TopicPartition('foo', 0)])
        # Give it a valid position so is_fetchable would otherwise be True.
        s.assignment[TopicPartition('foo', 0)].seek(
            OffsetAndMetadata(offset=0, metadata='', leader_epoch=-1))
        assert s.is_fetchable(TopicPartition('foo', 0))

        s.mark_pending_revocation({TopicPartition('foo', 0)})
        assert not s.is_fetchable(TopicPartition('foo', 0))
        # The pending-revocation flag is dropped along with the state
        # object on the next assign_from_subscribed that excludes the
        # partition (single-shot).
        s.assign_from_subscribed([TopicPartition('foo', 1)])
        s.assign_from_subscribed([
            TopicPartition('foo', 0), TopicPartition('foo', 1)])
        s.assignment[TopicPartition('foo', 0)].seek(
            OffsetAndMetadata(offset=0, metadata='', leader_epoch=-1))
        assert s.is_fetchable(TopicPartition('foo', 0))

    def test_mark_pending_revocation_ignores_unknown_partition(self):
        """mark_pending_revocation must not raise if a partition isn't
        currently assigned - the assignment may have shifted between
        the caller's snapshot and the call."""
        s = self._subscribed()
        s.assign_from_subscribed([TopicPartition('foo', 0)])
        s.mark_pending_revocation({
            TopicPartition('foo', 0), TopicPartition('foo', 99)})
        # Still gates the assigned one.
        assert s.assignment[TopicPartition('foo', 0)]._pending_revocation is True

    def test_pending_revocation_cleared_for_partition_retained_across_rebalance(self):
        """Regression test for #3131.

        Under the EAGER rebalance protocol the coordinator marks *every*
        currently-owned partition pending-revocation before JoinGroup (see
        ConsumerCoordinator._on_join_prepare_async). When the rebalance
        completes, ``assign_from_subscribed`` preserves the
        ``TopicPartitionState`` for any partition the member is given back,
        so a *retained* partition carries a stale ``_pending_revocation``
        flag. Because the flag is only ever cleared in
        ``TopicPartitionState.__init__``, such a partition reports
        not-fetchable forever - the fetcher positions it (ListOffsets) but
        never issues a FetchRequest - until a later rebalance drops it and
        creates fresh state.

        A partition retained across a rebalance must be fetchable again.
        """
        s = self._subscribed()
        foo0 = TopicPartition('foo', 0)
        foo1 = TopicPartition('foo', 1)
        s.assign_from_subscribed([foo0, foo1])
        for tp in (foo0, foo1):
            s.assignment[tp].seek(
                OffsetAndMetadata(offset=0, metadata='', leader_epoch=-1))
        assert s.is_fetchable(foo0)
        assert s.is_fetchable(foo1)

        # EAGER pre-rebalance: mark the full current assignment for revocation
        # (mirrors mark_pending_revocation(assigned_partitions())).
        s.mark_pending_revocation({foo0, foo1})
        assert not s.is_fetchable(foo0)
        assert not s.is_fetchable(foo1)

        # Rebalance completes and the member is assigned the same partitions
        # back. Their state (and valid position) is preserved.
        s.assign_from_subscribed([foo0, foo1])
        assert s.assignment[foo0].has_valid_position
        assert s.assignment[foo1].has_valid_position

        # They must be fetchable again - the revocation window is over.
        assert s.is_fetchable(foo0)
        assert s.is_fetchable(foo1)

    def test_preserves_preferred_read_replica_on_kept_partition(self):
        s = self._subscribed()
        s.assign_from_subscribed([TopicPartition('foo', 0)])
        s.assignment[TopicPartition('foo', 0)].update_preferred_read_replica(
            5, time.monotonic() + 60)
        assert s.assignment[TopicPartition('foo', 0)].preferred_read_replica() == 5

        s.assign_from_subscribed([
            TopicPartition('foo', 0), TopicPartition('foo', 1)])
        # KIP-392 cache survives.
        assert s.assignment[TopicPartition('foo', 0)].preferred_read_replica() == 5
        # New partition starts with no preferred replica.
        assert s.assignment[TopicPartition('foo', 1)].preferred_read_replica() is None

    def test_object_identity_for_kept_partition_state(self):
        """The actual TopicPartitionState instance must survive - not a
        copy with the same field values. This matters for callers (like
        the Fetcher) that hold references to TopicPartitionState
        instances across rebalances."""
        s = self._subscribed()
        s.assign_from_subscribed([TopicPartition('foo', 0)])
        original = s.assignment[TopicPartition('foo', 0)]
        s.assign_from_subscribed([
            TopicPartition('foo', 0), TopicPartition('foo', 1)])
        assert s.assignment[TopicPartition('foo', 0)] is original

    def test_revoked_partition_state_is_dropped(self):
        s = self._subscribed()
        s.assign_from_subscribed([TopicPartition('foo', 0), TopicPartition('foo', 1)])
        original = s.assignment[TopicPartition('foo', 1)]

        s.assign_from_subscribed([TopicPartition('foo', 0)])
        assert TopicPartition('foo', 1) not in s.assignment
        # The revoked TopicPartitionState is no longer referenced by the
        # subscription (caller may still hold the reference, but the
        # dict has dropped it).
        assert original not in s.assignment.values()

    def test_validates_topic_subscribed_before_mutation(self):
        """A bad topic must raise BEFORE any mutation. Half-applied
        state would leave the consumer in an inconsistent place."""
        s = self._subscribed()
        s.assign_from_subscribed([TopicPartition('foo', 0)])
        original_keys = list(s.assignment.keys())

        with pytest.raises(ValueError):
            s.assign_from_subscribed([
                TopicPartition('foo', 2),
                TopicPartition('not-subscribed', 0)])
        # Nothing was added or removed.
        assert list(s.assignment.keys()) == original_keys

    def test_full_replace_drops_all_old_partitions(self):
        """A new assignment with zero overlap drops every old partition."""
        s = self._subscribed()
        s.assign_from_subscribed([TopicPartition('foo', 0), TopicPartition('foo', 1)])
        s.assign_from_subscribed([TopicPartition('foo', 2)])
        assert set(s.assignment.keys()) == {TopicPartition('foo', 2)}

    def test_no_op_assignment_preserves_everything(self):
        """Same assignment in/out: every state field survives identical."""
        s = self._subscribed()
        s.assign_from_subscribed([TopicPartition('foo', 0), TopicPartition('foo', 1)])
        s.assignment[TopicPartition('foo', 0)].seek(
            OffsetAndMetadata(offset=50, metadata='', leader_epoch=-1))
        s.pause(TopicPartition('foo', 1))

        s.assign_from_subscribed([
            TopicPartition('foo', 0), TopicPartition('foo', 1)])

        assert s.assignment[TopicPartition('foo', 0)].position.offset == 50
        assert s.is_paused(TopicPartition('foo', 1))


# ---------------------------------------------------------------------------
# Reason-token pauses: pause_with_reason / resume_with_reason
# ---------------------------------------------------------------------------


class TestPauseWithReason:
    TP = TopicPartition('foo', 0)

    def _state(self):
        s = SubscriptionState()
        s.assign_from_user([self.TP])
        s.seek(self.TP, 0)
        return s

    def test_double_pause_requires_all_tokens_resumed(self):
        """Two independent pauses (rate-limit + maintenance): lifting one
        must not resume fetching while the other is still active."""
        s = self._state()
        s.pause_with_reason(self.TP, 'rate-limit')
        s.pause_with_reason(self.TP, 'maintenance')
        assert s.is_paused(self.TP)
        assert not s.is_fetchable(self.TP)
        assert s.paused_partitions() == {self.TP}

        s.resume_with_reason(self.TP, 'rate-limit')
        assert s.is_paused(self.TP)
        assert not s.is_fetchable(self.TP)
        assert s.paused_partitions() == {self.TP}
        assert s.pause_reasons(self.TP) == frozenset({'maintenance'})

        s.resume_with_reason(self.TP, 'maintenance')
        assert not s.is_paused(self.TP)
        assert s.is_fetchable(self.TP)
        assert s.paused_partitions() == set()
        assert s.pause_reasons(self.TP) == frozenset()

    def test_legacy_pause_resume_roundtrip_unchanged(self):
        """Pure legacy pause()/resume() keeps its pre-token semantics."""
        s = self._state()
        s.pause(self.TP)
        assert s.is_paused(self.TP)
        assert not s.is_fetchable(self.TP)
        s.resume(self.TP)
        assert not s.is_paused(self.TP)
        assert s.is_fetchable(self.TP)

    def test_legacy_resume_does_not_lift_reason_pause(self):
        s = self._state()
        s.pause(self.TP)
        s.pause_with_reason(self.TP, 'maintenance')
        s.resume(self.TP)
        assert s.is_paused(self.TP)
        assert not s.is_fetchable(self.TP)
        assert s.pause_reasons(self.TP) == frozenset({'maintenance'})
        s.resume_with_reason(self.TP, 'maintenance')
        assert not s.is_paused(self.TP)

    def test_reason_resume_does_not_lift_legacy_pause(self):
        s = self._state()
        s.pause(self.TP)
        s.pause_with_reason(self.TP, 'rate-limit')
        s.resume_with_reason(self.TP, 'rate-limit')
        assert s.is_paused(self.TP)
        assert not s.is_fetchable(self.TP)
        s.resume(self.TP)
        assert not s.is_paused(self.TP)

    def test_legacy_pause_not_reported_as_reason(self):
        s = self._state()
        s.pause(self.TP)
        assert s.is_paused(self.TP)
        assert s.pause_reasons(self.TP) == frozenset()

    def test_resume_unknown_reason_is_noop(self):
        s = self._state()
        s.pause_with_reason(self.TP, 'maintenance')
        s.resume_with_reason(self.TP, 'rate-limit')
        assert s.is_paused(self.TP)
        assert s.pause_reasons(self.TP) == frozenset({'maintenance'})

    def test_token_expires_on_monotonic_clock(self, fake_clock):
        s = self._state()
        s.pause_with_reason(self.TP, 'rate-limit', timeout_ms=1000)
        assert s.is_paused(self.TP)

        fake_clock.advance(0.5)
        assert s.is_paused(self.TP)
        assert not s.is_fetchable(self.TP)
        assert s.paused_partitions() == {self.TP}

        fake_clock.advance(0.6)
        assert not s.is_paused(self.TP)
        assert s.is_fetchable(self.TP)
        assert s.paused_partitions() == set()
        assert s.pause_reasons(self.TP) == frozenset()

    def test_expired_token_does_not_lift_other_tokens(self, fake_clock):
        s = self._state()
        s.pause_with_reason(self.TP, 'rate-limit', timeout_ms=1000)
        s.pause_with_reason(self.TP, 'maintenance')
        fake_clock.advance(2.0)
        # The timed token expired, but the indefinite one still holds.
        assert s.is_paused(self.TP)
        assert not s.is_fetchable(self.TP)
        assert s.pause_reasons(self.TP) == frozenset({'maintenance'})

    def test_renewal_not_lifted_by_previous_deadline(self, fake_clock):
        """Re-pausing the same reason replaces its deadline; the earlier
        deadline must not expire the renewed token."""
        s = self._state()
        s.pause_with_reason(self.TP, 'rate-limit', timeout_ms=1000)
        fake_clock.advance(0.9)
        # Manual renewal just before the original deadline.
        s.pause_with_reason(self.TP, 'rate-limit', timeout_ms=1000)

        fake_clock.advance(0.2)  # t=1.1: original deadline (t=1.0) passed
        assert s.is_paused(self.TP)
        assert not s.is_fetchable(self.TP)

        fake_clock.advance(0.9)  # t=2.0: renewed deadline (t=1.9) passed
        assert not s.is_paused(self.TP)
        assert s.is_fetchable(self.TP)

    def test_renewal_can_remove_expiry(self, fake_clock):
        s = self._state()
        s.pause_with_reason(self.TP, 'rate-limit', timeout_ms=1000)
        s.pause_with_reason(self.TP, 'rate-limit')  # renew without timeout
        fake_clock.advance(60.0)
        assert s.is_paused(self.TP)

    def test_tokens_dropped_when_partition_revoked(self, fake_clock):
        """Rebalance: tokens of a revoked partition must not affect its
        next owner - a fresh assignment starts unpaused."""
        s = SubscriptionState()
        s.subscribe(topics=['foo'])
        s.assign_from_subscribed([self.TP])
        s.seek(self.TP, 0)
        s.pause_with_reason(self.TP, 'maintenance')
        s.pause_with_reason(self.TP, 'rate-limit', timeout_ms=60 * 1000)
        assert s.is_paused(self.TP)

        # Revoked entirely, then re-assigned (new owner): fresh state.
        s.assign_from_subscribed([])
        assert not s.is_assigned(self.TP)
        s.assign_from_subscribed([self.TP])
        assert not s.is_paused(self.TP)
        assert s.pause_reasons(self.TP) == frozenset()
        s.seek(self.TP, 0)
        assert s.is_fetchable(self.TP)

    def test_tokens_kept_for_retained_partition(self):
        """A partition that stays assigned across a rebalance keeps its
        tokens (same owner)."""
        s = SubscriptionState()
        s.subscribe(topics=['foo'])
        s.assign_from_subscribed([self.TP, TopicPartition('foo', 1)])
        s.pause_with_reason(self.TP, 'maintenance')
        s.assign_from_subscribed([self.TP])
        assert s.is_paused(self.TP)
        assert s.pause_reasons(self.TP) == frozenset({'maintenance'})

    def test_pause_reasons_unassigned_partition(self):
        s = SubscriptionState()
        assert s.pause_reasons(self.TP) == frozenset()

    def test_reason_validation(self):
        s = self._state()
        with pytest.raises(TypeError):
            s.pause_with_reason(self.TP, None)
        with pytest.raises(TypeError):
            s.pause_with_reason(self.TP, 42)
        with pytest.raises(ValueError):
            s.pause_with_reason(self.TP, '')
        with pytest.raises(TypeError):
            s.resume_with_reason(self.TP, None)

    def test_timeout_validation(self):
        s = self._state()
        with pytest.raises(ValueError):
            s.pause_with_reason(self.TP, 'rate-limit', timeout_ms=0)
        with pytest.raises(ValueError):
            s.pause_with_reason(self.TP, 'rate-limit', timeout_ms=-5)
