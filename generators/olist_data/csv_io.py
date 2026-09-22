from __future__ import annotations

import csv
from typing import Iterator, TextIO


MAX_RECORD_CHARS = 65_536


class _RecordLines:
    def __init__(self, stream: TextIO, limit: int) -> None:
        self._stream = stream
        self._limit = limit
        self.consumed = 0

    def __iter__(self) -> _RecordLines:
        return self

    def __next__(self) -> str:
        remaining = self._limit - self.consumed
        line = self._stream.readline(remaining + 1)
        if len(line) > remaining:
            raise ValueError("CSV record exceeds character limit")
        if not line:
            raise StopIteration
        self.consumed += len(line)
        return line


def bounded_csv_reader(
    stream: TextIO,
    *,
    max_chars: int = MAX_RECORD_CHARS,
) -> Iterator[list[str]]:
    if max_chars < 1:
        raise ValueError("CSV record character limit must be positive")
    lines = _RecordLines(stream, max_chars)
    reader = csv.reader(lines, strict=True)
    first = True
    while True:
        lines.consumed = 0
        record = next(reader, None)
        if record is None:
            return
        if first and record:
            record[0] = record[0].removeprefix("\ufeff")
        first = False
        yield record
