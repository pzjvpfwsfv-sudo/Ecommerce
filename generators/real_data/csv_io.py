import csv
from typing import TextIO


MAX_RECORD_CHARS = 65536


class _RecordLines:
    def __init__(self, stream: TextIO, limit: int):
        self.stream = stream
        self.limit = limit
        self.consumed = 0

    def __iter__(self):
        return self

    def __next__(self):
        remaining = self.limit - self.consumed
        line = self.stream.readline(remaining + 1)
        if len(line) > remaining:
            raise ValueError("CSV record exceeds character limit")
        if not line:
            raise StopIteration
        self.consumed += len(line)
        return line


def bounded_csv_reader(stream: TextIO, *, max_chars: int = MAX_RECORD_CHARS):
    if max_chars < 1:
        raise ValueError("CSV record character limit must be positive")
    lines = _RecordLines(stream, max_chars)
    reader = csv.reader(lines, strict=True)
    while True:
        # A quoted multiline record shares one budget, before csv builds its fields.
        lines.consumed = 0
        record = next(reader, None)
        if record is None:
            return
        yield record
