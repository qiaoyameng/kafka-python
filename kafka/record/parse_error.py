"""Structured errors for batch-level record parsing.

When a compressed record batch is corrupt on the wire (bad CRC, garbage
payload, mangled length header), the consumer used to raise a plain
``CorruptRecordError`` carrying little more than an offset.  Recovery code
then had no way to know how many bytes/records it could safely discard.

This module makes that information explicit:

* :class:`BatchParseError` records *where* the failure happened
  (topic/partition/base offset, the byte bounds of the candidate batch) and
  *when* it happened (:data:`ParseStage`).
* ``boundary_reliable`` tells whether the candidate batch bounds came straight
  from an intact length header.  When the length header itself is damaged the
  bounds are a guess and the batch must never be skipped.
* :class:`CorruptBatchDecision` / :func:`default_corrupt_batch_handler` give
  consumers an explicit quarantine point that may fail (default) or skip the
  whole well-delimited batch.

Offsets of records *inside* a batch that cannot be decoded are never invented.
"""

from kafka.errors import CorruptRecordError


class ParseStage:
    """Points at which parsing a record batch can fail.

    Attributes:
        LENGTH: The Int32 length prefix is missing, truncated, negative or
            otherwise implausible. Batch boundaries cannot be located.
        HEADER: The length header is intact (bounds are known) but the fixed
            batch header (magic, attributes, count ...) cannot be parsed.
        CRC: The batch/record checksum does not match. Bounds are known but
            the payload cannot be trusted.
        DECOMPRESS: The compression wrapper is intact (CRC passes) but the
            compressed payload cannot be decoded.
        RECORDS: Structural failure while walking the records inside an
            otherwise well-delimited batch.
    """

    LENGTH = "length"
    HEADER = "header"
    CRC = "crc"
    DECOMPRESS = "decompress"
    RECORDS = "records"


class CorruptBatchDecision:
    """Decisions a quarantine callback may return."""

    FAIL = "fail"
    SKIP_BATCH = "skip_batch"


def default_corrupt_batch_handler(error):
    """Default quarantine policy: never silently drop data.

    Returns :attr:`CorruptBatchDecision.FAIL` unconditionally, which causes the
    original :class:`BatchParseError` to be raised to the caller.
    """
    return CorruptBatchDecision.FAIL


class BatchParseError(CorruptRecordError):
    """Structured failure while parsing one record batch.

    Arguments:
        message: Human readable description.
        stage: One of :class:`ParseStage`, identifying where parsing failed.
        topic (str or None): Topic name, if known at the failure layer
            (the record layer itself does not know it).
        partition (int or None): Partition id, if known.
        base_offset (int or None): Absolute base/offset field read for the
            candidate batch. Never the offset of an inner record that could
            not be decoded; ``None`` only if even the offset is unreadable.
        batch_start (int or None): Byte offset of the batch within the fetch
            buffer, when available.
        batch_end (int or None): Byte offset immediately past the candidate
            batch within the fetch buffer, when the boundary is reliable.
        boundary_reliable (bool): True only when ``batch_start``/``batch_end``
            were derived from an intact length header. False for
            :data:`ParseStage.LENGTH` failures, where guessing a boundary is
            forbidden.
    """

    def __init__(self, message, stage,
                 topic=None, partition=None, base_offset=None,
                 batch_start=None, batch_end=None, boundary_reliable=False):
        super().__init__(message)
        self.stage = stage
        self.topic = topic
        self.partition = partition
        self.base_offset = base_offset
        self.batch_start = batch_start
        self.batch_end = batch_end
        self.boundary_reliable = boundary_reliable

    @property
    def skippable(self):
        """True only when the whole batch may safely be skipped.

        Skipping requires reliable byte boundaries. A damaged length header
        (:data:`ParseStage.LENGTH`) is never skippable because there is no
        sound place to resume from.
        """
        return (
            self.boundary_reliable
            and self.stage != ParseStage.LENGTH
            and self.batch_start is not None
            and self.batch_end is not None
        )

    def with_location(self, topic, partition, batch_start=None, batch_end=None,
                      base_offset=None):
        """Return a copy enriched with fetch-layer location information."""
        enriched = BatchParseError(
            str(self),
            self.stage,
            topic=topic,
            partition=partition,
            base_offset=self.base_offset if base_offset is None else base_offset,
            batch_start=self.batch_start if batch_start is None else batch_start,
            batch_end=self.batch_end if batch_end is None else batch_end,
            boundary_reliable=self.boundary_reliable,
        )
        return enriched

    def __str__(self):
        loc = self.topic if self.topic is not None else "<unknown topic>"
        if self.partition is not None:
            loc = "%s-%s" % (loc, self.partition)
        bounds = (
            "bytes[%s:%s]" % (self.batch_start, self.batch_end)
            if self.boundary_reliable else "unknown bounds"
        )
        return (
            "%s: stage=%s topic_partition=%s base_offset=%s %s: %s" % (
                self.__class__.__name__,
                self.stage,
                loc,
                self.base_offset,
                bounds,
                self.args[0] if self.args else "",
            )
        )
