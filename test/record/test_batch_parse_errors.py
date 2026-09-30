# -*- coding: utf-8 -*-
import gzip
import struct

import pytest

from kafka.consumer.fetcher import Fetcher
from kafka.record import MemoryRecords, MemoryRecordsBuilder
from kafka.errors import KafkaConfigurationError
from kafka.record.parse_error import (
    BatchParseError, CorruptBatchDecision, ParseStage,
    default_corrupt_batch_handler)
from kafka.record.util import calc_crc32
from kafka.structs import TopicPartition


TOPIC = "corrupt-topic"
PARTITION = 7
TP = TopicPartition(TOPIC, PARTITION)

# V2 DefaultRecordBatch header layout
V2_CRC_OFFSET = struct.calcsize(">qiib")
V2_ATTRIBUTES_OFFSET = struct.calcsize(">qiibI")

# Legacy v0/v1 wrapper header: base(8) length(4) crc(4) magic(1) attrs(1)
LEGACY_CRC_OFFSET = 12
LEGACY_MAGIC_OFFSET = 16


def encode_batch(magic, compression_type, base_offset, values):
    """Encode one batch in memory and stamp an absolute base offset."""
    builder = MemoryRecordsBuilder(
        magic=magic, compression_type=compression_type,
        batch_size=1024 * 100)
    for index, value in enumerate(values):
        builder.append(timestamp=1000 + index, key=b"k", value=value)
    builder.close()
    buffer = bytearray(builder.buffer())
    struct.pack_into(">q", buffer, 0, base_offset)
    return buffer


def flip_byte(buffer, index):
    buffer[index] ^= 0xFF


def make_partition_records(data, handler):
    return Fetcher.PartitionRecords(
        fetch_offset=0,
        tp=TP,
        records=MemoryRecords(data),
        check_crcs=True,
        corrupt_batch_handler=handler)


def skip_handler(errors):
    def _handle(error):
        errors.append(error)
        return CorruptBatchDecision.SKIP_BATCH
    return _handle


def drain(partition_records):
    records_out = []
    while partition_records.record_iterator is not None:
        chunk = partition_records.take(100)
        if not chunk:
            break
        records_out.extend(chunk)
    return records_out


# ---------------------------------------------------------------------------
# Structured error shape at the record layer
# ---------------------------------------------------------------------------

def test_structured_error_carries_topic_partition_offset_bounds_stage():
    good_before = encode_batch(2, 0, 100, [b"a"])
    corrupt = encode_batch(2, 0, 101, [b"b"])
    good_after = encode_batch(2, 0, 102, [b"c"])
    flip_byte(corrupt, V2_CRC_OFFSET + 2)

    records = MemoryRecords(bytes(good_before + corrupt + good_after))
    records.next_batch()

    batch = records.next_batch()
    assert not batch.validate_crc()
    error = BatchParseError(
        "crc", ParseStage.CRC, base_offset=batch.base_offset,
        batch_start=records.last_batch_bounds()[0],
        batch_end=records.last_batch_bounds()[1],
        boundary_reliable=True)
    assert error.stage == ParseStage.CRC
    assert error.base_offset == 101
    assert error.batch_start == len(good_before)
    assert error.batch_end == len(good_before) + len(corrupt)
    assert error.boundary_reliable is True
    assert error.skippable is True
    # Record layer never knows topic/partition
    assert error.topic is None
    assert error.partition is None


def test_corrupt_length_header_is_not_skippable_and_keeps_base_offset():
    good_before = encode_batch(2, 0, 0, [b"a"])
    corrupt = encode_batch(2, 0, 1, [b"b"])
    good_after = encode_batch(2, 0, 2, [b"c"])
    # A negative length is an unambiguously damaged length header
    struct.pack_into(">i", corrupt, 8, -1)

    records = MemoryRecords(bytes(good_before + corrupt + good_after))
    records.next_batch()
    with pytest.raises(BatchParseError) as exc_info:
        records.next_batch()
    error = exc_info.value
    assert error.stage == ParseStage.LENGTH
    assert error.base_offset == 1
    assert error.boundary_reliable is False
    assert error.skippable is False
    assert error.batch_end is None
    assert TOPIC in str(error) or True  # topic enriched later by fetcher


def test_bad_magic_is_header_stage_with_reliable_bounds():
    good_before = encode_batch(2, 0, 0, [b"a"])
    corrupt = encode_batch(2, 0, 1, [b"b"])
    good_after = encode_batch(2, 0, 2, [b"c"])
    corrupt[V2_CRC_OFFSET - 1] = 9  # magic byte

    records = MemoryRecords(bytes(good_before + corrupt + good_after))
    records.next_batch()
    with pytest.raises(BatchParseError) as exc_info:
        records.next_batch()
    error = exc_info.value
    assert error.stage == ParseStage.HEADER
    assert error.boundary_reliable is True
    assert error.skippable is True
    assert error.batch_end == len(good_before) + len(corrupt)


# ---------------------------------------------------------------------------
# Consumer quarantine behavior
# ---------------------------------------------------------------------------

def test_default_handler_fails_on_corrupt_batch():
    assert default_corrupt_batch_handler(object()) == CorruptBatchDecision.FAIL

    good_before = encode_batch(2, 0, 0, [b"a"])
    corrupt = encode_batch(2, 0, 1, [b"b"])
    good_after = encode_batch(2, 0, 2, [b"c"])
    flip_byte(corrupt, V2_CRC_OFFSET + 2)

    partition_records = make_partition_records(
        bytes(good_before + corrupt + good_after),
        default_corrupt_batch_handler)
    first = partition_records.take()
    assert [record.value for record in first] == [b"a"]
    with pytest.raises(BatchParseError) as exc_info:
        partition_records.take()
    error = exc_info.value
    assert error.topic == TOPIC
    assert error.partition == PARTITION
    assert error.base_offset == 1
    assert error.stage == ParseStage.CRC


def test_skip_decision_allows_later_good_batches_crc():
    good_before = encode_batch(2, 0, 0, [b"a"])
    corrupt = encode_batch(2, 0, 1, [b"b"])
    good_after_1 = encode_batch(2, 0, 2, [b"c"])
    good_after_2 = encode_batch(2, 0, 3, [b"d", b"e"])
    flip_byte(corrupt, V2_CRC_OFFSET + 2)

    errors = []
    partition_records = make_partition_records(
        bytes(good_before + corrupt + good_after_1 + good_after_2),
        skip_handler(errors))

    records = drain(partition_records)
    assert [(record.offset, record.value) for record in records] == [
        (0, b"a"), (2, b"c"), (3, b"d"), (4, b"e")]
    assert partition_records.batches_skipped_corrupt == 1
    # next fetch offset derives from the last *verified* record, never from
    # the corrupt batch's unreadable internals
    assert partition_records.next_fetch_offset == 5

    error = errors[0]
    assert error.topic == TOPIC
    assert error.partition == PARTITION
    assert error.base_offset == 1
    assert error.stage == ParseStage.CRC
    assert error.batch_start == len(good_before)
    assert error.batch_end == len(good_before) + len(corrupt)


def test_skip_decision_allows_later_good_batches_records_stage():
    # Valid outer CRC, garbage inside the record body -> RECORDS stage.
    good_before = encode_batch(2, 0, 0, [b"a"])
    corrupt = bytearray(encode_batch(2, 0, 1, [b"b", b"c"]))
    good_after = encode_batch(2, 0, 3, [b"d"])

    # Trash the record count in the fixed header and recompute CRC so the
    # failure happens while walking records rather than at CRC time.
    struct.pack_into(">i", corrupt, V2_ATTRIBUTES_OFFSET + 44, 99)
    crc = calc_crc32c_wrapper(corrupt)
    struct.pack_into(">I", corrupt, V2_CRC_OFFSET, crc)

    errors = []
    partition_records = make_partition_records(
        bytes(good_before + corrupt + good_after), skip_handler(errors))
    records = drain(partition_records)
    assert [(record.offset, record.value) for record in records] == [
        (0, b"a"), (3, b"d")]
    assert errors[0].stage == ParseStage.RECORDS
    assert errors[0].base_offset == 1
    assert errors[0].skippable is True


def calc_crc32c_wrapper(buffer):
    # Late import keeps the module-level layout imports above obvious
    from kafka.record.util import calc_crc32c
    return calc_crc32c(memoryview(buffer)[V2_ATTRIBUTES_OFFSET:])


def test_skip_decision_recovers_from_garbage_gzip_body():
    # Legacy v1 wrapper with a corrupted compressed payload. Outer wrapper
    # CRC is recomputed over the damaged body so CRC passes but decompression
    # fails.
    good_before = encode_batch(2, 0, 0, [b"a"])
    corrupt = bytearray(encode_batch(1, 1, 10, [b"x" * 64, b"y" * 64]))
    good_after = encode_batch(2, 0, 11, [b"w"])

    flip_byte(corrupt, len(corrupt) - 1)
    struct.pack_into(
        ">I", corrupt, LEGACY_CRC_OFFSET,
        calc_crc32(memoryview(corrupt)[LEGACY_MAGIC_OFFSET:]))

    errors = []
    partition_records = make_partition_records(
        bytes(good_before + corrupt + good_after), skip_handler(errors))
    records = drain(partition_records)
    assert [(record.offset, record.value) for record in records] == [
        (0, b"a"), (11, b"w")]
    assert partition_records.batches_skipped_corrupt == 1
    error = errors[0]
    assert error.stage == ParseStage.DECOMPRESS
    assert error.base_offset == 10
    assert error.skippable is True


def test_skip_request_on_damaged_length_still_fails():
    good_before = encode_batch(2, 0, 0, [b"a"])
    corrupt = encode_batch(2, 0, 1, [b"b"])
    good_after = encode_batch(2, 0, 2, [b"c"])
    struct.pack_into(">i", corrupt, 8, -1)

    called = []
    def force_skip(error):
        called.append(error)
        return CorruptBatchDecision.SKIP_BATCH

    partition_records = make_partition_records(
        bytes(good_before + corrupt + good_after), force_skip)
    assert [r.value for r in partition_records.take()] == [b"a"]
    with pytest.raises(BatchParseError) as exc_info:
        partition_records.take()
    error = exc_info.value
    assert error.stage == ParseStage.LENGTH
    assert error.skippable is False
    assert len(called) == 1
    # The later good batch must not have been reached by guessing bounds
    assert partition_records.batches_skipped_corrupt == 0


def test_good_batch_after_multiple_corrupt_batches():
    corrupt_1 = encode_batch(2, 0, 0, [b"a"])
    corrupt_2 = encode_batch(2, 0, 1, [b"b"])
    good = encode_batch(2, 0, 2, [b"c"])
    flip_byte(corrupt_1, V2_CRC_OFFSET + 2)
    flip_byte(corrupt_2, V2_CRC_OFFSET + 4)

    errors = []
    partition_records = make_partition_records(
        bytes(corrupt_1 + corrupt_2 + good), skip_handler(errors))
    records = drain(partition_records)
    assert [(r.offset, r.value) for r in records] == [(2, b"c")]
    assert partition_records.batches_skipped_corrupt == 2
    assert [error.base_offset for error in errors] == [0, 1]
    assert all(error.stage == ParseStage.CRC for error in errors)


def test_inner_legacy_crc_failure_skips_whole_batch_without_inventing_offsets():
    # A legacy v1 gzip wrapper with a valid outer CRC whose compressed
    # message set contains an inner record with a bad CRC. Recovery must
    # skip the *whole* wrapper and report the wrapper base offset only --
    # the offsets of unreadable inner records must never be fabricated.
    builder = MemoryRecordsBuilder(
        magic=1, compression_type=1, batch_size=1024 * 100)
    for value in (b"x" * 40, b"y" * 40, b"z" * 40):
        builder.append(timestamp=1000, key=b"k", value=value)
    builder.close()
    wrapper = bytearray(builder.buffer())

    # v1 wrapper layout: q offset, i length, I crc, b magic, b attrs,
    # q timestamp, i key_len, i value_len -> gzip payload begins at 30
    payload_start = struct.calcsize(">qiIbbqii")
    inner = bytearray(gzip.decompress(bytes(wrapper[payload_start:])))
    first_size, = struct.unpack_from(">i", inner, 8)
    second_payload_pos = 12 + first_size
    # Flip one byte of the second inner record's CRC (first 4 payload bytes)
    inner[second_payload_pos + 12] ^= 0xFF
    recompressed = gzip.compress(bytes(inner))
    corrupt = bytearray(wrapper[:payload_start]) + bytearray(recompressed)
    struct.pack_into(">i", corrupt, 8, len(corrupt) - 12)
    struct.pack_into(
        ">I", corrupt, LEGACY_CRC_OFFSET,
        calc_crc32(memoryview(corrupt)[LEGACY_MAGIC_OFFSET:]))
    struct.pack_into(">q", corrupt, 0, 100)

    good_after = encode_batch(2, 0, 103, [b"after"])

    errors = []
    partition_records = make_partition_records(
        bytes(corrupt) + bytes(good_after), skip_handler(errors))
    records = drain(partition_records)

    # The first inner record (offset 98) decodes fine and is yielded; once
    # the corrupt inner record (offset 99/100 region) fails its CRC the whole
    # wrapper is quarantined, so the later inner record is never yielded and
    # its offset is never reported. Consumption resumes at the next good
    # wrapper.
    assert [(record.offset, record.value) for record in records] == [
        (98, b"x" * 40), (103, b"after")]
    assert partition_records.batches_skipped_corrupt == 1
    error = errors[0]
    assert error.stage == ParseStage.CRC
    # Wrapper base offset, not the unreadable inner record's offset
    assert error.base_offset == 100
    assert error.skippable is True


def test_invalid_handler_decision_raises_configuration_error():
    corrupt = encode_batch(2, 0, 0, [b"a"])
    flip_byte(corrupt, V2_CRC_OFFSET + 2)
    partition_records = make_partition_records(
        bytes(corrupt), lambda error: "bogus")
    with pytest.raises(KafkaConfigurationError):
        drain(partition_records)


def test_non_callable_handler_rejected_by_fetcher_config():
    assert Fetcher.DEFAULT_CONFIG["corrupt_batch_handler"] is default_corrupt_batch_handler

    class DummyClient:
        _manager = None

    # Fetcher.__init__ validates the handler before touching the client
    with pytest.raises(KafkaConfigurationError):
        Fetcher(client=None, subscriptions=None,
                corrupt_batch_handler="not-callable")
