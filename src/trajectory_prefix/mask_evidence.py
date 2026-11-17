"""Explain each actual removal, checking which placeholder labels are necessary."""

import json
from collections import Counter

from .deep_review import NodeReader
from .mask_job import POLICY
from .mask_verification import raw_relaxed_covers
from .masking import DISABLED_LABELS, markers, message_matches, message_strings
from .projection import project


def explain_partition(rows, storage):
    reader = NodeReader(storage)
    for row in rows:
        if row["status"] != "removed":
            continue
        ar, br = row["record"], row["retained"]
        a, b = reader.node(ar), reader.node(br)
        ap, bp = [project(n, r["dataset"], r["date"], POLICY) for n, r in ((a, ar), (b, br))]
        if not raw_relaxed_covers(a, b, ar, br, POLICY):
            raise ValueError("Explanation has no independent witness")
        different = [(i, json.loads(x), json.loads(y)) for i, (x, y) in
                     enumerate(zip(ap.tokens, bp.tokens)) if x != y]
        labels = set()
        details = []
        for i, x, y in different:
            left, right = dict(message_strings(x)), dict(message_strings(y))
            for field in sorted(left.keys() & right.keys()):
                text, other = left[field], right[field]
                if text == other:
                    continue
                labels.update(m.group() for value in (text, other) for m in markers(value))
                pos = 0
                while pos < min(len(text), len(other)) and text[pos] == other[pos]:
                    pos += 1
                details.append({"message_index": i, "role": x["role"], "field": field,
                                "lengths": [len(text), len(other)], "first_difference": pos,
                                "short_excerpt": text[max(0, pos - 80):pos + 240],
                                "retained_excerpt": other[max(0, pos - 80):pos + 240]})
        necessary = []
        for label in sorted(labels):
            token = DISABLED_LABELS.set(frozenset({label}))
            try:
                if not all(message_matches(x, y) for _, x, y in different):
                    necessary.append(label)
            finally:
                DISABLED_LABELS.reset(token)
        source_masks = {"[USER]", "[TMP]", "[INTERNAL_NAME_REDACTED]"}
        token = DISABLED_LABELS.set(frozenset(labels - source_masks))
        try:
            source_only = all(message_matches(x, y) for _, x, y in different)
        finally:
            DISABLED_LABELS.reset(token)
        yield {"record": ar, "retained": br, "model": row["model"],
               "message_counts": [len(ap.tokens), len(bp.tokens)],
               "has_session": bool((a.get("metadata") or {}).get("session_id") not in
                                   (None, "", "no_session_id")),
               "necessary_labels": necessary, "labels_in_different_fields": sorted(labels),
               "source_mask_labels_suffice": source_only, "differences": details}


def summarize_partition(rows):
    counts = Counter()
    for row in rows:
        counts[("all", "removed")] += 1
        counts[("all", "source_mask_labels_suffice")] += row["source_mask_labels_suffice"]
        counts[("all", "with_session" if row["has_session"] else "without_session")] += 1
        for label in row["necessary_labels"]:
            counts[("necessary_labels", label)] += 1
        for diff in row["differences"]:
            key = diff["role"] + "/" + ("arguments" if diff["field"].startswith("tool_calls/") else "content")
            counts[("different_fields", key)] += 1
    yield from counts.items()
