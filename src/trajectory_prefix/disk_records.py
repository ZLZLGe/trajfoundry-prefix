"""Run-local SQLite collections for million-record publication metadata."""

import json
import sqlite3
from collections.abc import Mapping
from pathlib import Path


def decode_index_record(line):
    """Return the JSON payload for SQLite and the fields to index."""
    try:
        text = line.decode("utf-8")
        return text, json.loads(text)
    except (UnicodeDecodeError, json.JSONDecodeError):
        # Keep the byte decoder's support for BOMs and other JSON encodings.
        return line, json.loads(line)


def _part_records(part):
    with part.open("rb") as stream:
        for line in stream:
            if line.strip():
                yield decode_index_record(line)


class DiskRecords(Mapping):
    def __init__(self, path):
        self.db = sqlite3.connect(path)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=NORMAL")
        self.db.execute("PRAGMA cache_size=-65536")
        self.db.execute("CREATE TABLE entries (path TEXT PRIMARY KEY, payload TEXT NOT NULL, "
                        "status TEXT, identifier TEXT UNIQUE)")

    def add(self, path, value, identifier=None):
        payload = json.dumps(value, separators=(",", ":"))
        self.add_serialized(path, payload, value.get("status"), identifier)

    def add_serialized(self, path, payload, status, identifier):
        """Index a JSON line already decoded and checked by the caller."""
        self.db.execute("INSERT INTO entries VALUES(?,?,?,?)",
                        (path, payload, status, identifier))

    def __getitem__(self, key):
        row = self.db.execute("SELECT payload FROM entries WHERE path=?", (key,)).fetchone()
        if row is None:
            raise KeyError(key)
        return json.loads(row[0])

    def __contains__(self, key):
        return self.db.execute("SELECT 1 FROM entries WHERE path=?", (key,)).fetchone() is not None

    def __iter__(self):
        return (row[0] for row in self.db.execute("SELECT path FROM entries ORDER BY path"))

    def __len__(self):
        return self.db.execute("SELECT count(*) FROM entries").fetchone()[0]

    def values(self):
        return (json.loads(row[0]) for row in self.db.execute("SELECT payload FROM entries"))

    def scan_items(self):
        """Read each stored key and payload together in one table scan."""
        for path, payload in self.db.execute("SELECT path, payload FROM entries"):
            yield path, json.loads(payload)

    def distinct_identifier_count(self):
        # A distinct subquery counts a missing identifier once, like a Python set.
        return self.db.execute("SELECT count(*) FROM (SELECT DISTINCT identifier FROM entries)").fetchone()[0]

    def view(self, status=None):
        return DiskView(self, status)

    def retained_identifiers(self):
        """Stream retained paths and trajectory IDs in Spark's sorted lineage order."""
        return self.db.execute("SELECT path, identifier FROM entries WHERE status='kept' ORDER BY path")

    def commit(self):
        self.db.commit()

    def verify_retained_copies(self, directory):
        """Check each copy against its retained path and trajectory ID exactly once."""
        # SQLite assigns rowids as inspection records are appended. One byte per
        # row tracks coverage without indexing the full copy payload a second time.
        last_rowid = self.db.execute("SELECT coalesce(max(rowid), 0) FROM entries").fetchone()[0]
        seen = bytearray(last_rowid + 1)
        lookup = self.db.cursor()
        copied = 0
        for part in sorted(Path(directory).glob("part-*")):
            for _, row in _part_records(part):
                match = lookup.execute(
                    "SELECT rowid FROM entries WHERE path=? AND identifier=? AND status='kept'",
                    (row["path"], row["trajectory_id"]),
                ).fetchone()
                if match is None or seen[match[0]]:
                    raise ValueError("Normalized copy receipt does not uniquely match a retained trajectory")
                seen[match[0]] = 1
                copied += 1
        if copied != len(self.view("kept")):
            raise ValueError("Normalized copy receipts are incomplete")
        return copied

    @classmethod
    def from_parts(cls, path, directory):
        result = cls(path)
        for part in sorted(Path(directory).glob("part-*")):
            for payload, row in _part_records(part):
                result.add_serialized(row["path"], payload, row.get("status"), row["trajectory_id"])
            result.commit()
        return result


class DiskView:
    def __init__(self, records, status):
        self.records, self.status = records, status

    def query(self, count=False):
        sql = "SELECT " + ("count(*)" if count else "payload") + " FROM entries"
        if self.status is not None:
            return self.records.db.execute(sql + " WHERE status=?", (self.status,))
        return self.records.db.execute(sql)

    def __len__(self):
        return self.query(True).fetchone()[0]

    def __iter__(self):
        return (json.loads(row[0]) for row in self.query())
