"""Run-local SQLite collections for million-record publication metadata."""

import json
import sqlite3
from collections.abc import Mapping
from pathlib import Path


class DiskRecords(Mapping):
    def __init__(self, path):
        self.db = sqlite3.connect(path)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=NORMAL")
        self.db.execute("CREATE TABLE entries (path TEXT PRIMARY KEY, payload TEXT NOT NULL, "
                        "status TEXT, identifier TEXT UNIQUE)")

    def add(self, path, value, identifier=None):
        self.db.execute("INSERT INTO entries VALUES(?,?,?,?)",
                        (path, json.dumps(value, separators=(",", ":")), value.get("status"), identifier))

    def __getitem__(self, key):
        row = self.db.execute("SELECT payload FROM entries WHERE path=?", (key,)).fetchone()
        if row is None:
            raise KeyError(key)
        return json.loads(row[0])

    def __iter__(self):
        return (row[0] for row in self.db.execute("SELECT path FROM entries ORDER BY path"))

    def __len__(self):
        return self.db.execute("SELECT count(*) FROM entries").fetchone()[0]

    def values(self):
        return (json.loads(row[0]) for row in self.db.execute("SELECT payload FROM entries"))

    def view(self, status=None):
        return DiskView(self, status)

    def commit(self):
        self.db.commit()

    @classmethod
    def from_parts(cls, path, directory):
        result = cls(path)
        for part in sorted(Path(directory).glob("part-*")):
            with part.open() as stream:
                for line in stream:
                    if line.strip():
                        row = json.loads(line)
                        result.add(row["path"], row, row["trajectory_id"])
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
