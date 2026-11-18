"""Retain the source-file and physical-row accounting of whole-dialogue archives."""

from collections import Counter


def verify_archive_sources(archive, sources, origin_count, file_count):
    inventory = {entry["key"]: entry["bytes"] for entry in archive["inventory"]}
    if (file_count != len(inventory) or file_count != len(archive["inventory"])
            or origin_count != archive["totals"]["input_rows"]):
        raise ValueError("Archive file or raw row count disagrees with lineage")
    if sources.filter(lambda r: r[0] not in inventory or not 0 <= r[1] < inventory[r[0]]).take(1):
        raise ValueError("Archive row refers to an unknown object or invalid offset")
    if sources.map(lambda r: (r[0], r[1])).distinct().count() != origin_count:
        raise ValueError("Conflicting hashes at the same archive byte offset")


def summarize(rows):
    counts, rewrites = Counter(), Counter()
    for row in rows:
        values = row["archive"]
        counts["trajectories"] += 1
        counts["accepted" if row["strict"] else "quarantined"] += 1
        counts["messages"] += values["messages"]
        counts.update(row["metrics"])
        counts["source_success_true"] += values["success"] is True
        counts["source_success_false"] += values["success"] is False
        rewrites.update(values["rewrites"])
    return counts, rewrites


def published_archive(archive, inputs, retained):
    original, original_rewrites = summarize(inputs)
    source_totals = archive["totals"]
    if (any(source_totals.get(k, 0) != n for k, n in original.items())
            or original["trajectories"] + source_totals.get("duplicate_rows", 0)
            != source_totals["input_rows"]
            or original_rewrites != Counter(archive["rewrites"])):
        raise ValueError("Inspected trajectories disagree with the archive manifest")
    counts, rewrites = summarize(retained)
    totals = dict(source_totals)
    for key in original.keys() | counts.keys():
        if key in totals or counts[key]:
            totals[key] = counts[key]
    totals["duplicate_rows"] = totals["input_rows"] - counts["trajectories"]
    return {**archive, "totals": totals, "rewrites": dict(rewrites)}
