"""Stream the files array of a standard manifest without loading it into RAM."""

import json


class ManifestStream:
    def __init__(self, stream, chunk_size=1024 * 1024):
        self.stream, self.chunk_size = stream, chunk_size
        self.buffer, self.offset, self.eof = "", 0, False
        self.metadata = {}
        self.decoder = json.JSONDecoder()

    def fill(self):
        self.buffer = self.buffer[self.offset:]
        self.offset = 0
        text = self.stream.read(self.chunk_size)
        self.buffer += text
        self.eof = not text

    def peek(self):
        while True:
            while self.offset < len(self.buffer) and self.buffer[self.offset].isspace():
                self.offset += 1
            if self.offset < len(self.buffer):
                return self.buffer[self.offset]
            if self.eof:
                return ""
            self.fill()

    def expect(self, char):
        if self.peek() != char:
            raise ValueError("Malformed manifest: expected " + char)
        self.offset += 1

    def value(self):
        if not self.peek():
            raise ValueError("Unexpected end of manifest")
        while True:
            try:
                value, end = self.decoder.raw_decode(self.buffer, self.offset)
                # Numbers may continue in the next buffer.
                if end == len(self.buffer) and not self.eof:
                    self.fill()
                    continue
                self.offset = end
                return value
            except json.JSONDecodeError:
                if self.eof:
                    raise
                self.fill()

    def files(self):
        seen = set()
        self.expect("{")
        first = True
        while self.peek() != "}":
            if not first:
                self.expect(",")
            key = self.value()
            if not isinstance(key, str) or key in seen:
                raise ValueError("Duplicate or invalid manifest key")
            seen.add(key)
            self.expect(":")
            if key == "files":
                self.expect("[")
                first_file = True
                while self.peek() != "]":
                    if not first_file:
                        self.expect(",")
                    entry = self.value()
                    if not isinstance(entry, dict):
                        raise ValueError("Invalid manifest file entry")
                    yield entry
                    first_file = False
                self.expect("]")
            else:
                self.metadata[key] = self.value()
            first = False
        self.expect("}")
        if self.peek() or "files" not in seen:
            raise ValueError("Missing files or trailing manifest content")
