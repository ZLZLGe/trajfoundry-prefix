"""Sorted prefix scan with disk-backed pending records and transitive receipts."""

from __future__ import annotations

import json
import pickle
import sqlite3
import zlib
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path

from .projection import Projection, covers, digest


@dataclass
class Record:
    reference: dict
    projection: Projection

    @property
    def key(self):
        return (
            digest(self.projection.scope),
            self.projection.vector,
            -self.reference.get("bytes", 0),
            self.reference["id"],
            self.projection.mergeable,
        )


class PrefixScan:
    """The active path contains only row IDs, never full trajectory payloads."""

    def __init__(self, path: Path):
        self.db = sqlite3.connect(path)
        self.db.execute("PRAGMA journal_mode=OFF")
        self.db.execute("PRAGMA synchronous=OFF")
        self.db.execute(
            "CREATE TABLE records (id INTEGER PRIMARY KEY, ref TEXT, projection BLOB, parent INTEGER)"
        )
        self.active: dict[int, list[int]] = {}
        self.previous: tuple | None = None
        self.last_vector = b""
        self.last_scope = b""

    def close(self):
        self.db.close()

    def load(self, row_id: int) -> Projection:
        (payload,) = self.db.execute(
            "SELECT projection FROM records WHERE id=?", (row_id,)
        ).fetchone()
        return pickle.loads(zlib.decompress(payload))

    def insert(self, record: Record):
        key = record.key
        if self.previous is not None and key < self.previous:
            raise ValueError("PrefixScan requires sorted records")
        self.previous = key
        p = record.projection
        scope_hash = digest(p.scope)
        if scope_hash != self.last_scope:
            self.active.clear()
        else:
            limit = min(len(self.last_vector), len(p.vector))
            common = 0
            while (
                common < limit
                and self.last_vector[common : common + 32] == p.vector[common : common + 32]
            ):
                common += 32
            for length in list(self.active):
                if length > common:
                    del self.active[length]
        self.last_scope, self.last_vector = scope_hash, p.vector
        row_id = self.db.execute(
            "INSERT INTO records(ref,projection) VALUES(?,?)",
            (
                json.dumps(record.reference, ensure_ascii=False),
                zlib.compress(pickle.dumps(p, protocol=5), 1),
            ),
        ).lastrowid
        if not p.mergeable:
            return
        # Prefer an already retained representative for equal transcripts when
        # it contains everything in the new record. Otherwise the richer one wins.
        for candidate in self.active.get(len(p.vector), []):
            previous = self.load(candidate)
            if covers(p, previous):
                self.db.execute(
                    "UPDATE records SET parent=? WHERE id=?", (candidate, row_id)
                )
                return
        for length in sorted(self.active):
            remaining = []
            for candidate in self.active[length]:
                previous = self.load(candidate)
                if covers(previous, p):
                    self.db.execute(
                        "UPDATE records SET parent=? WHERE id=?", (row_id, candidate)
                    )
                else:
                    remaining.append(candidate)
            self.active[length] = remaining
        self.active.setdefault(len(p.vector), []).append(row_id)

    def results(self) -> Iterator[dict]:
        self.db.commit()
        for row_id, ref, parent in self.db.execute(
            "SELECT id,ref,parent FROM records ORDER BY id"
        ):
            reference = json.loads(ref)
            if parent is None:
                p = self.load(row_id)
                yield {
                    "status": "kept",
                    "record": reference,
                    "isolation_reason": p.isolation_reason,
                    "changes": list(p.changes),
                }
                continue
            seen = {row_id}
            while True:
                if parent in seen:
                    raise RuntimeError("Internal coverage cycle")
                seen.add(parent)
                next_parent, target = self.db.execute(
                    "SELECT parent,ref FROM records WHERE id=?", (parent,)
                ).fetchone()
                if next_parent is None:
                    break
                parent = next_parent
            short, long = self.load(row_id), self.load(parent)
            # Verify transitive witness against the actual final retained record.
            if not covers(short, long):
                raise RuntimeError("Final retained record does not cover removed record")
            self.db.execute("UPDATE records SET parent=? WHERE id=?", (parent, row_id))
            yield {
                "status": "removed",
                "record": reference,
                "retained": json.loads(target),
                "reason": "equal" if len(short.tokens) == len(long.tokens) else "prefix",
                "changes": sorted(set(short.changes) | set(long.changes)),
            }


def deduplicate(records: Iterable[Record], workspace: Path) -> list[dict]:
    scan = PrefixScan(workspace / "prefix.sqlite")
    try:
        for record in sorted(records, key=lambda r: r.key):
            scan.insert(record)
        return list(scan.results())
    finally:
        scan.close()
