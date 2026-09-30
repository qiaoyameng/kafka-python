# -*- coding: utf-8 -*-
"""Structured batch-parse errors and explicit quarantine decisions.

Covers the scenario described for corrupted compressed records: when a batch
is damaged the consumer gets a CorruptBatchError carrying topic, partition,
wire-level base offset, batch byte boundaries and the failure stage; a
callback may skip the whole batch only when its boundary is reliably known,
and a damaged length header never permits a skip.
"""
import struct

import pytest

from kafka.errors import (
    CorruptBatchError, CorruptRecordError,
    STAGE_CRC, STAGE_DECOMPRESS, STAGE_FRAMING, STAGE_RECORDS,
    QUARANTINE_FAIL, QUARANTINE_SKIP_BATCH, default_corrupt_batch_handler,
)
from kafka.record import MemoryRecords, MemoryRecordsBuilder
from kafka.structs import TopicPartition
from kafka.consumer.fetcher import Fetcher


TP = TopicPartition("topic-corrupt", 3)


def _build_batch(magic, compression_type=0, offset=0, values=(b"a", b"b")):
    builder = MemoryRecordsBuilder(
        magic=magic, compression_type=compression_type,
        batch_size=9999999, offset=offset)
    for i, value in enumerate(values):
        builder.append(key=None, value=value,
                       timestamp=1000 + i if magic >= 1 else None,
                       headers=[] if magic >= 2 else None)
    builder.close()
    return bytearray(builder.buffer())


def _partition_records(data, handler=default_corrupt_batch_handler):
    return Fetcher.PartitionRecords(
        0, TP, MemoryRecords(bytes(data)),
        check_crcs=True, corrupt_batch_handler=handler)


def _drain(partition_records):
    out = []
    while True:
        batch = partition_records.record_iterator
        if batch is None:
            break
        got = partition_records.take(100)
        if not got:
            break
        out.extend(got)
    return out


# ---------------------------------------------------------------------------
# MemoryRecords: framing detection
# ---------------------------------------------------------------------------

def test_framing_error_detects_huge_length():
    good = _build_batch(magic=2, offset=10, values=(b"x",))
    corrupt = bytearray(20)
    corrupt[0:8] = struct.pack(">q", 99)
    corrupt[8:12] = struct.pack(">i", 10_000_000)  # runs way past the buffer
    data = bytes(good) + bytes(corrupt)

    records = MemoryRecords(data)
    assert records.has_next()  # the good batch is still reachable
    assert records.next_batch() is not None
    assert records.has_next() is False
    framing = records.framing_error()
    assert framing is not None
    start, declared = framing
    assert declared == 10_000_000
    assert records.base_offset_at(start) == 99


def test_framing_error_detects_negative_length():
    corrupt = bytearray(16)
    corrupt[8:12] = struct.pack(">i", -5)
    records = MemoryRecords(bytes(corrupt))
    assert records.has_next() is False
    start, declared = records.framing_error()
    assert declared == -5
    assert start == 0


def test_short_tail_is_not_a_framing_error():
    # A partial log-overhead tail is a truncated fetch, not corruption.
    data = bytes(_build_batch(magic=2)) + b"\x00\x00\x00"
    records = MemoryRecords(data)
    records.next_batch()
    assert records.framing_error() is None


def test_current_batch_range_tracks_bytes():
    first = _build_batch(magic=2, offset=0, values=(b"a",))
    second = _build_batch(magic=2, offset=1, values=(b"b",))
    records = MemoryRecords(bytes(first) + bytes(second))
    batch = records.next_batch()
    assert records.current_batch_range() == (0, len(first))
    assert batch.base_offset == 0
    records.next_batch()
    assert records.current_batch_range() == (len(first), len(first) + len(second))


# ---------------------------------------------------------------------------
# Default behavior: raise a structured CorruptBatchError
# ---------------------------------------------------------------------------

def _corrupt_crc(data, magic):
    # CRC location is shared across magic 0/1 (12) and magic 2 (21).
    pos = 21 if magic == 2 else 12
    data[pos] ^= 0xFF
    return data


def test_corrupt_crc_default_raises_structured_error():
    # The v2 client builder writes base offset 0 with relative deltas, as a
    # broker-facing message; that wire base offset is what the error reports.
    first = _build_batch(magic=2, values=(b"ok",))
    bad = _corrupt_crc(_build_batch(magic=2, values=(b"bad",)), 2)
    third = _build_batch(magic=2, values=(b"later",))
    records = _partition_records(bytes(first) + bytes(bad) + bytes(third))

    assert records.take(1)[0].value == b"ok"
    with pytest.raises(CorruptBatchError) as exc_info:
        records.take(1)
    err = exc_info.value
    assert err.topic == TP.topic
    assert err.partition == TP.partition
    assert err.stage == STAGE_CRC
    assert err.base_offset == 0
    assert err.batch_start == len(first)
    assert err.batch_end == len(first) + len(bad)
    # Header-derived v2 resume offset (last offset delta + 1).
    assert err.next_offset == 1
    assert err.boundary_reliable is True
    assert isinstance(err, CorruptRecordError)


def test_skip_handler_resumes_at_following_good_batch():
    first = _build_batch(magic=2, values=(b"ok",))
    bad = _corrupt_crc(_build_batch(magic=2, values=(b"bad",)), 2)
    third = _build_batch(magic=2, values=(b"later",))
    seen = []

    def handler(error):
        seen.append(error)
        return QUARANTINE_SKIP_BATCH

    records = _partition_records(
        bytes(first) + bytes(bad) + bytes(third), handler=handler)

    values = [r.value for r in _drain(records)]
    assert values == [b"ok", b"later"]
    assert len(seen) == 1
    assert seen[0].stage == STAGE_CRC
    assert seen[0].base_offset == 0
    assert seen[0].boundary_reliable is True
    # Resume position is the header-derived next offset of the bad batch.
    assert records.next_fetch_offset == 1


def test_fail_handler_decision_raises():
    bad = _corrupt_crc(_build_batch(magic=2, offset=0, values=(b"bad",)), 2)
    records = _partition_records(bad, handler=lambda e: QUARANTINE_FAIL)
    with pytest.raises(CorruptBatchError):
        records.take(1)


def test_none_decision_is_treated_as_fail():
    bad = _corrupt_crc(_build_batch(magic=2, offset=0, values=(b"bad",)), 2)
    records = _partition_records(bad, handler=lambda e: None)
    with pytest.raises(CorruptBatchError):
        records.take(1)


def test_invalid_decision_raises():
    bad = _corrupt_crc(_build_batch(magic=2, offset=0, values=(b"bad",)), 2)
    records = _partition_records(bad, handler=lambda e: "nope")
    with pytest.raises(Exception):
        records.take(1)


# ---------------------------------------------------------------------------
# Corrupt compressed body (v1 gzip wrapper)
# ---------------------------------------------------------------------------

V1_VALUE_OFFSET = 34  # 8+4+4+1+1+8 header + 4 key-len + 4 value-len


def _broker_style_v1_wrapper(values):
    """Build a magic-1 gzip wrapper with the broker-style outer base offset.

    A 0.10+ broker rewrites the wrapper offset to the last inner offset. The
    offset field is not covered by the record CRC, so patching it does not
    invalidate CRC validation.
    """
    start_offset = 4
    bad = _build_batch(magic=1, compression_type=1, offset=start_offset,
                       values=values)
    last_inner = start_offset + len(values) - 1
    bad[0:8] = struct.pack(">q", last_inner)
    return bad, last_inner


def test_corrupt_compressed_body_stage_and_skip():
    first = _build_batch(magic=1, compression_type=0, offset=0,
                         values=(b"plain",))
    bad, last_inner = _broker_style_v1_wrapper((b"c0", b"c1", b"c2"))
    # Flipping a gzip payload byte invalidates the wrapper CRC, so with CRC
    # checking on the reported failure stage is 'crc'.
    bad[V1_VALUE_OFFSET + 10] ^= 0xFF
    third = _build_batch(magic=1, compression_type=0, offset=last_inner + 1,
                         values=(b"after",))

    seen = []
    records = _partition_records(
        bytes(first) + bytes(bad) + bytes(third),
        handler=lambda e: seen.append(e) or QUARANTINE_SKIP_BATCH)
    values = [r.value for r in _drain(records)]
    assert values == [b"plain", b"after"]
    assert seen[0].stage == STAGE_CRC  # wrapper CRC protects the body
    assert seen[0].base_offset == last_inner
    # magic 1 wrapper base offset is the last inner offset -> +1 resumes past
    # the whole compressed batch.
    assert seen[0].next_offset == last_inner + 1


def test_client_style_v1_wrapper_with_zero_base_is_not_skippable():
    # Pre-0.10-style builders leave the wrapper offset at 0; the header then
    # cannot identify the resume point, so skipping is refused even on request.
    bad = _build_batch(magic=1, compression_type=1, offset=4,
                       values=(b"c0", b"c1", b"c2"))
    bad[V1_VALUE_OFFSET + 10] ^= 0xFF

    def handler(error):
        assert error.boundary_reliable is False
        assert error.next_offset is None
        return QUARANTINE_SKIP_BATCH

    records = _partition_records(bytes(bad), handler=handler)
    with pytest.raises(CorruptBatchError) as exc_info:
        records.take(1)
    assert exc_info.value.boundary_reliable is False


def test_corrupt_compressed_body_decompress_stage_when_crc_disabled():
    bad, last_inner = _broker_style_v1_wrapper((b"c0", b"c1", b"c2"))
    bad[V1_VALUE_OFFSET + 10] ^= 0xFF
    seen = []
    records = Fetcher.PartitionRecords(
        0, TP, MemoryRecords(bytes(bad)), check_crcs=False,
        corrupt_batch_handler=lambda e: seen.append(e) or QUARANTINE_FAIL)
    with pytest.raises(CorruptBatchError) as exc_info:
        records.take(1)
    assert exc_info.value.stage == STAGE_DECOMPRESS
    assert exc_info.value.base_offset == last_inner
    assert exc_info.value.boundary_reliable is True
    assert exc_info.value.next_offset == last_inner + 1
    assert exc_info.value.cause is not None
    assert len(seen) == 1


# ---------------------------------------------------------------------------
# Length header corruption: never skippable, even when callback says skip
# ---------------------------------------------------------------------------

def test_corrupt_length_header_always_fails_even_with_skip_callback():
    first = _build_batch(magic=2, offset=0, values=(b"ok",))
    corrupt = bytearray(20)
    corrupt[0:8] = struct.pack(">q", 50)  # readable wire base offset
    corrupt[8:12] = struct.pack(">i", 9_999_999)
    third = _build_batch(magic=2, values=(b"unreachable",))

    def handler(error):
        assert error.stage == STAGE_FRAMING
        assert error.base_offset == 50
        assert error.batch_end is None
        assert error.next_offset is None
        assert error.boundary_reliable is False
        return QUARANTINE_SKIP_BATCH  # must be ignored

    records = _partition_records(
        bytes(first) + bytes(corrupt) + bytes(third), handler=handler)
    values = []
    with pytest.raises(CorruptBatchError) as exc_info:
        for record in records.record_iterator:
            values.append(record.value)
    assert values == [b"ok"]
    err = exc_info.value
    assert err.stage == STAGE_FRAMING
    assert err.boundary_reliable is False
    assert err.batch_end is None
    # No fabricated resume offset.
    assert err.next_offset is None


def test_corrupt_length_header_default_fails():
    corrupt = bytearray(16)
    corrupt[8:12] = struct.pack(">i", -1)
    records = _partition_records(bytes(corrupt))
    with pytest.raises(CorruptBatchError) as exc_info:
        records.take(1)
    assert exc_info.value.stage == STAGE_FRAMING


# ---------------------------------------------------------------------------
# Undecodable inner record structure
# ---------------------------------------------------------------------------

def test_undecodable_inner_record_does_not_fabricate_offsets():
    # v0 uncompressed batch with a valid wrapper CRC but truncated key length
    # is hard to build directly; instead corrupt a payload byte inside a v2
    # batch with CRC disabled so parsing reaches record decoding.
    good = _build_batch(magic=2, values=(b"keep",))
    bad = _build_batch(magic=2, values=(b"payload",))
    # Corrupt the first varint (record length) inside the Records section,
    # which makes the record structure undecodable.
    bad[61] ^= 0xFF
    later = _build_batch(magic=2, values=(b"after",))
    seen = []

    def handler(error):
        seen.append(error)
        return QUARANTINE_SKIP_BATCH

    records = Fetcher.PartitionRecords(
        0, TP, MemoryRecords(bytes(good) + bytes(bad) + bytes(later)),
        check_crcs=False, corrupt_batch_handler=handler)
    values = [r.value for r in records.record_iterator]
    # The structurally corrupt batch is skipped; following batches survive.
    assert values == [b"keep", b"after"]
    err = seen[0]
    assert err.stage == STAGE_RECORDS
    # Only the wire-level batch base offset is exposed (never a fabricated
    # internal record offset); resume comes from the batch header.
    assert err.base_offset == 0
    assert err.next_offset == 1
    assert err.boundary_reliable is True

    # And the same damage with a FAIL decision raises.
    records_fail = Fetcher.PartitionRecords(
        0, TP, MemoryRecords(bytes(bad)), check_crcs=False,
        corrupt_batch_handler=lambda e: QUARANTINE_FAIL)
    with pytest.raises(CorruptBatchError) as exc_info:
        list(records_fail.record_iterator)
    assert exc_info.value.stage == STAGE_RECORDS
    assert exc_info.value.base_offset == 0


def test_legacy_uncompressed_batch_skip_recovers_following_batch():
    def build(values, offset):
        builder = MemoryRecordsBuilder(
            magic=1, compression_type=0, batch_size=9999999, offset=offset)
        for value in values:
            builder.append(timestamp=1000, key=None, value=value)
        builder.close()
        return bytearray(builder.buffer())

    first = build([b"a"], 0)
    bad = build([b"b"], 1)
    bad[12] ^= 0xFF  # CRC byte
    third = build([b"c"], 2)

    seen = []
    records = Fetcher.PartitionRecords(
        0, TP, MemoryRecords(bytes(first) + bytes(bad) + bytes(third)),
        corrupt_batch_handler=lambda e: seen.append(e) or QUARANTINE_SKIP_BATCH)

    values = [r.value for r in records.record_iterator]
    assert values == [b"a", b"c"]
    assert seen[0].stage == STAGE_CRC
    assert seen[0].base_offset == 1
    assert seen[0].next_offset == 2
    assert seen[0].batch_start == len(first)
    assert seen[0].batch_end == len(first) + len(bad)
    assert records.next_fetch_offset == 3


def test_memory_records_framing_first_batch_identified():
    # Length header corrupt on the very first batch: has_next() is False but
    # framing_error() must still expose it instead of looking like an empty /
    # oversized response.
    corrupt = bytearray(30)
    corrupt[0:8] = struct.pack(">q", 7)
    corrupt[8:12] = struct.pack(">i", 10_000)
    records = MemoryRecords(bytes(corrupt))
    assert records.has_next() is False
    framing = records.framing_error()
    assert framing == (0, 10_000)
    assert records.base_offset_at(framing[0]) == 7
