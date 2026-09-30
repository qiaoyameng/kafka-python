# This class takes advantage of the fact that all formats v0, v1 and v2 of
# messages storage has the same byte offsets for Length and Magic fields.
# Lets look closely at what leading bytes all versions have:
#
# V0 and V1 (Offset is MessageSet part, other bytes are Message ones):
#  Offset => Int64
#  BytesLength => Int32
#  CRC => Int32
#  Magic => Int8
#  ...
#
# V2:
#  BaseOffset => Int64
#  Length => Int32
#  PartitionLeaderEpoch => Int32
#  Magic => Int8
#  ...
#
# So we can iterate over batches just by knowing offsets of Length. Magic is
# used to construct the correct class for Batch itself.

import struct

from kafka.errors import CorruptRecordError, IllegalStateError, UnsupportedVersionError
from kafka.record.abc import ABCRecords
from kafka.record.legacy_records import LegacyRecordBatch, LegacyRecordBatchBuilder
from kafka.record.default_records import DefaultRecordBatch, DefaultRecordBatchBuilder


class MemoryRecords(ABCRecords):

    LENGTH_OFFSET = struct.calcsize(">q")
    LOG_OVERHEAD = struct.calcsize(">qi")
    MAGIC_OFFSET = struct.calcsize(">qii")

    # Minimum space requirements for Record V0
    MIN_SLICE = LOG_OVERHEAD + LegacyRecordBatch.RECORD_OVERHEAD_V0

    __slots__ = ("_buffer", "_pos", "_next_slice", "_remaining_bytes",
                 "_next_start", "_next_end", "_current_start", "_current_end",
                 "_framing_error")

    def __init__(self, bytes_data):
        self._buffer = bytes_data
        self._pos = 0
        # We keep one slice ahead so `has_next` will return very fast
        self._next_slice = None
        self._remaining_bytes = None
        # Byte positions backing the sliced batches, plus any framing failure
        # (a length header that is present but impossible to satisfy).
        self._next_start = None
        self._next_end = None
        self._current_start = None
        self._current_end = None
        self._framing_error = None
        self._cache_next()

    def size_in_bytes(self):
        return len(self._buffer)

    def valid_bytes(self):
        # We need to read the whole buffer to get the valid_bytes.
        # NOTE: in Fetcher we do the call after iteration, so should be fast
        if self._remaining_bytes is None:
            next_slice = self._next_slice
            next_start = self._next_start
            next_end = self._next_end
            current_start = self._current_start
            current_end = self._current_end
            framing_error = self._framing_error
            pos = self._pos
            while self._remaining_bytes is None:
                self._cache_next()
            # Reset previous iterator position
            self._next_slice = next_slice
            self._next_start = next_start
            self._next_end = next_end
            self._current_start = current_start
            self._current_end = current_end
            self._framing_error = framing_error
            self._pos = pos
        return len(self._buffer) - self._remaining_bytes

    def framing_error(self):
        """Return a description of an unreliable framing point, or None.

        A non-None result means a complete length header is present but its
        declared length can never point to a valid batch inside the buffer
        (negative, or extending past the end of the available bytes). The end
        of that batch cannot be located by scanning, so callers must not
        attempt to skip over it.

        Returns: tuple (start_byte, declared_length) or None.
        """
        return self._framing_error

    def base_offset_at(self, pos):
        """Read the wire-level base offset of the batch header at ``pos``.

        The offset is read directly from the batch header bytes; it is never
        inferred from internal records that could not be decoded. Requires at
        least the log overhead to be available at ``pos``.
        """
        base_offset, = struct.unpack_from(">q", self._buffer, pos)
        return base_offset

    # NOTE: we cache offsets here as kwargs for a bit more speed, as cPython
    # will use LOAD_FAST opcode in this case
    def _cache_next(self, len_offset=LENGTH_OFFSET, log_overhead=LOG_OVERHEAD):
        buffer = self._buffer
        buffer_len = len(buffer)
        pos = self._pos
        remaining = buffer_len - pos
        if remaining < log_overhead:
            # Will be re-checked in Fetcher for remaining bytes.
            self._remaining_bytes = remaining
            self._next_slice = None
            self._next_start = None
            self._next_end = None
            self._framing_error = None
            return

        length, = struct.unpack_from(
            ">i", buffer, pos + len_offset)

        slice_end = pos + log_overhead + length
        # A negative length, or a declared batch that runs past the available
        # bytes, cannot be reconciled with the buffer. This is distinct from a
        # partially delivered log-overhead tail above: here the length header
        # itself is untrustworthy, so the boundary of the batch is unknown and
        # must not be guessed.
        if length < 0 or slice_end > buffer_len:
            # The first eight bytes (the batch base offset) are still
            # readable, but the batch end is unknown.
            # Will be re-checked in Fetcher for remaining bytes
            self._remaining_bytes = remaining
            self._next_slice = None
            self._next_start = None
            self._next_end = None
            self._framing_error = (pos, length)
            return

        self._framing_error = None
        self._next_start = pos
        self._next_slice = memoryview(buffer)[pos: slice_end]
        self._pos = slice_end
        self._next_end = slice_end

    def has_next(self):
        return self._next_slice is not None

    # NOTE: same cache for LOAD_FAST as above
    def next_batch(self, _min_slice=MIN_SLICE,
                   _magic_offset=MAGIC_OFFSET):
        next_slice = self._next_slice
        if next_slice is None:
            return None
        # Record the byte span of the batch we are about to hand out so
        # callers can quarantine it using a reliably known boundary.
        self._current_start = self._next_start
        self._current_end = self._next_end
        if len(next_slice) < _min_slice:
            raise CorruptRecordError(
                "Record size is less than the minimum record overhead "
                "({})".format(_min_slice - self.LOG_OVERHEAD))
        self._cache_next()
        magic, = struct.unpack_from(">b", next_slice, _magic_offset)
        if magic <= 1:
            return LegacyRecordBatch(next_slice, magic)
        else:
            return DefaultRecordBatch(next_slice)

    def __iter__(self):
        return self

    def __next__(self):
        if not self.has_next():
            raise StopIteration
        return self.next_batch()

    def current_batch_range(self):
        """Byte range (start, end) in the buffer of the last batch returned.

        Available right after :meth:`next_batch`; start is the position of the
        batch base offset and end is the first byte of the following batch.
        Returns None if no complete batch has been yielded.
        """
        if self._current_start is None:
            return None
        return self._current_start, self._current_end

    next = __next__


class MemoryRecordsBuilder:

    __slots__ = ("_builder", "_batch_size", "_buffer", "_next_offset", "_closed",
                 "_magic", "_bytes_written", "_producer_id", "_producer_epoch",
                 "_base_sequence")

    def __init__(self, magic, compression_type, batch_size, offset=0,
                 transactional=False, producer_id=-1, producer_epoch=-1, base_sequence=-1):
        assert magic in [0, 1, 2], "Not supported magic"
        assert compression_type in [0, 1, 2, 3, 4], "Not valid compression type"
        if magic >= 2:
            assert not transactional or producer_id != -1, "Cannot write transactional messages without a valid producer ID"
            assert producer_id == -1 or producer_epoch != -1, "Invalid negative producer epoch"
            assert producer_id == -1 or base_sequence != -1, "Invalid negative sequence number used"

            self._builder = DefaultRecordBatchBuilder(
                magic=magic, compression_type=compression_type,
                is_transactional=transactional, producer_id=producer_id,
                producer_epoch=producer_epoch, base_sequence=base_sequence,
                batch_size=batch_size)
            self._producer_id = producer_id
            self._producer_epoch = producer_epoch
            self._base_sequence = base_sequence
        else:
            assert not transactional and producer_id == -1, "Idempotent messages are not supported for magic %s" % (magic,)
            self._builder = LegacyRecordBatchBuilder(
                magic=magic, compression_type=compression_type,
                batch_size=batch_size)
            self._producer_id = None
            self._producer_epoch = None
            self._base_sequence = None
        self._batch_size = batch_size
        self._buffer = None

        self._next_offset = offset
        self._closed = False
        self._magic = magic
        self._bytes_written = 0

    def skip(self, offsets_to_skip):
        # Exposed for testing compacted records
        self._next_offset += offsets_to_skip

    def append(self, timestamp, key, value, headers=None):
        """ Append a message to the buffer.

        Returns: RecordMetadata or None if unable to append
        """
        if headers is None:
            headers = []
        if self._closed:
            return None

        offset = self._next_offset
        metadata = self._builder.append(offset, timestamp, key, value, headers)
        # Return of None means there's no space to add a new message
        if metadata is None:
            return None

        self._next_offset += 1
        return metadata

    def set_producer_state(self, producer_id, producer_epoch, base_sequence, is_transactional):
        if self._magic < 2:
            raise UnsupportedVersionError('Producer State requires Message format v2+')
        elif self._closed:
            # Sequence numbers are assigned when the batch is closed while the accumulator is being drained.
            # If the resulting ProduceRequest to the partition leader failed for a retriable error, the batch will
            # be re queued. In this case, we should not attempt to set the state again, since changing the pid and sequence
            # once a batch has been sent to the broker risks introducing duplicates.
            raise IllegalStateError("Trying to set producer state of an already closed batch. This indicates a bug on the client.")
        self._builder.set_producer_state(producer_id, producer_epoch, base_sequence, is_transactional)
        self._producer_id = producer_id
        self._producer_epoch = producer_epoch
        self._base_sequence = base_sequence

    @property
    def producer_id(self):
        return self._producer_id

    @property
    def producer_epoch(self):
        return self._producer_epoch

    @property
    def base_sequence(self):
        return self._base_sequence

    def records(self):
        assert self._closed
        return MemoryRecords(self._buffer)

    def close(self):
        # This method may be called multiple times on the same batch
        # i.e., on retries
        # we need to make sure we only close it out once
        # otherwise compressed messages may be double-compressed
        # see Issue 718
        if not self._closed:
            self._bytes_written = self._builder.size()
            # Keep the buffer as bytearray to avoid a full-batch copy on
            # close. Downstream consumers (MemoryRecords via memoryview and
            # the protocol encoder via slice-assignment) handle bytearray
            # without further copies.
            self._buffer = self._builder.build()
            if self._magic == 2:
                self._producer_id = self._builder.producer_id
                self._producer_epoch = self._builder.producer_epoch
                self._base_sequence = self._builder.base_sequence
            self._builder = None
        self._closed = True

    def size_in_bytes(self):
        if not self._closed:
            return self._builder.size()
        else:
            return len(self._buffer)

    def compression_rate(self):
        assert self._closed
        return self.size_in_bytes() / self._bytes_written

    def is_full(self):
        if self._closed:
            return True
        else:
            return self._builder.size() >= self._batch_size

    def next_offset(self):
        return self._next_offset

    def buffer(self):
        assert self._closed
        return self._buffer
