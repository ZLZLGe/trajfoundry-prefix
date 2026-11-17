"""Full immutable-input census, relaxed pair evaluation and retained recheck."""

from __future__ import annotations

import json
import time
import zipfile
from collections import Counter
from dataclasses import asdict
from pathlib import Path

import orjson

from .deep_review import NodeReader
from .mask_verification import raw_relaxed_covers
from .masking import (
    MAX_SPAN,
    TEMPLATES,
    VERSION,
    census_message,
    choose_group,
    group_candidates,
    message_matches,
    message_shape,
    message_strings,
    string_bound,
)
from .projection import Policy, canonical, digest, project
from .publication import write_json
from .storage import Reader, file_sha256, reference
from .verification import raw_covers

POLICY = Policy(extra_information="messages_only")


def validate_baseline(receipt, expected):
    path = Path(receipt["report"])
    if file_sha256(path) != receipt["report_sha256"] or not (path.parent / "SUCCEEDED").is_file():
        raise ValueError("Baseline recheck changed or did not complete")
    report = json.loads(path.read_text())
    if (report["removed_count"] != 0 or report["input_count"] != len(expected)
            or report["kept_count"] != len(expected)
            or report["policy"] != json.loads(json.dumps(asdict(POLICY)))):
        raise ValueError("Baseline must be a complete zero-removal same-policy recheck")
    inputs = Path(report["input_plan"])
    if file_sha256(inputs) != report["input_plan_sha256"]:
        raise ValueError("Baseline retained inventory changed")
    seen = set()
    with inputs.open() as f:
        for line in f:
            row = json.loads(line)
            if row["path"] in seen or {k: row[k] for k in ("path", "bytes", "sha256")} != expected.get(row["path"]):
                raise ValueError("Published baseline differs from the rechecked retained set")
            seen.add(row["path"])
    if seen != expected.keys():
        raise ValueError("Baseline recheck does not cover the source partition")


def index_partition(entries, storage):
    reader = Reader(storage)
    for entry in entries:
        ref = reference(json.loads(entry) if isinstance(entry, str) else entry)
        node = reader.node(ref)
        p = project(node, ref["dataset"], ref["date"], POLICY)
        messages = [orjson.loads(t) for t in p.tokens]
        labels, templates, flags = Counter(), Counter(), []
        for m in messages:
            active, blocked = census_message(m)
            labels.update(active)
            templates.update(blocked)
            flags.append(bool(active))
        shapes = tuple(digest(canonical(message_shape(m))) for m in messages)
        group = digest(p.scope + b"".join(shapes[:2])).hex() if p.mergeable else ref["id"]
        yield {"ref": ref, "group": group, "model": node.get("model", ""),
               "message_count": len(messages), "mask_count": sum(labels.values()),
               "labels": dict(labels), "templates": dict(templates),
               "mask_flags": tuple(flags), "hashes": tuple(digest(t) for t in p.tokens),
               "shapes": shapes, "isolation": p.isolation_reason,
               "string_bounds": tuple({key: string_bound(text) for key, text in message_strings(m)}
                                      for m in messages)}


def census_partition(rows):
    counts = Counter()
    for row in rows:
        counts[("all", "input")] += 1
        counts[("model", row["model"])] += 1
        counts[("all", "with_marker")] += bool(row["mask_count"])
        counts[("all", "eligible_with_marker")] += bool(row["mask_count"] and not row["isolation"])
        counts[("all", "with_template")] += bool(row["templates"])
        if row["isolation"]:
            counts[("isolation", row["isolation"])] += 1
        for kind in ("labels", "templates"):
            for (field, label), n in row[kind].items():
                counts[(kind, field + ":" + label)] += n
                counts[(kind + "_trajectories", field + ":" + label)] += 1
    yield from counts.items()


def nested_counts(pairs):
    result = {}
    for (group, key), count in pairs:
        result.setdefault(group, {})[key] = count
    return result


def check_partition(pairs, storage):
    reader = NodeReader(storage)
    for pair in pairs:
        ar, br = pair["short"], pair["long"]
        aa, bb = reader.node(ar), reader.node(br)
        ap, bp = (project(n, r["dataset"], r["date"], POLICY) for n, r in ((aa, ar), (bb, br)))
        if not ap.mergeable or not bp.mergeable or ap.scope != bp.scope or len(ap.tokens) > len(bp.tokens):
            raise ValueError("Candidate eligibility or identity changed")
        am, bm = [orjson.loads(t) for t in ap.tokens], [orjson.loads(t) for t in bp.tokens]
        differences = [i for i, (a, b) in enumerate(zip(ap.tokens, bp.tokens)) if a != b]
        if differences != pair["different_messages"]:
            raise ValueError("Candidate differences changed")
        matched = all(message_matches(am[i], bm[i]) for i in differences)
        outside_bound = False
        if not matched:
            outside_bound = all(message_matches(am[i], bm[i],
                               max(len(ap.tokens[i]), len(bp.tokens[i]))) for i in differences)
        fields, labels = Counter(), Counter()
        for i in differences:
            left, right = dict(message_strings(am[i])), dict(message_strings(bm[i]))
            for field in left.keys() & right.keys():
                if left[field] != right[field]:
                    fields[am[i]["role"] + "/" + ("arguments" if field.startswith("tool_calls/") else field)] += 1
            for msg in (am[i], bm[i]):
                for (_, label), n in census_message(msg)[0].items():
                    labels[label] += n
        yield {**pair, "matched": matched, "outside_bound_only": outside_bound,
               "fields": dict(fields), "labels": dict(labels)}


def audit_partition(decisions, storage):
    reader = NodeReader(storage)
    for row in decisions:
        if row["status"] != "removed":
            continue
        ar, br = row["record"], row["retained"]
        a, b = reader.node(ar), reader.node(br)
        if not raw_relaxed_covers(a, b, ar, br, POLICY):
            raise ValueError("Independent raw-object relaxed witness failed")
        if raw_covers(a, b, ar, br, POLICY):
            raise ValueError("Unexpected ordinary prefix in the frozen retained baseline")
        yield {"record": ar, "retained": br, "passed": True}


def run(spark, input_plan, output, storage, partitions, source_manifest):
    import trajectory_prefix

    sc = spark.sparkContext
    if not sc.master.startswith("k8s://"):
        raise ValueError("Business masking evaluation requires Spark Cluster")
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    started = time.monotonic()

    def event(name, **values):
        row = {"event": name, "elapsed_seconds": round(time.monotonic() - started, 2), **values}
        with (output / "progress.jsonl").open("a") as f:
            f.write(json.dumps(row) + "\n")
        print(json.dumps(row), flush=True)

    package = output / "mask-evaluation-package.zip"
    with zipfile.ZipFile(package, "w", zipfile.ZIP_DEFLATED) as z:
        base = Path(trajectory_prefix.__file__).parent
        for f in base.glob("*.py"):
            z.write(f, "trajectory_prefix/" + f.name)
    sc.addPyFile(str(package))
    manifest_ref = reference(source_manifest)
    source = json.loads(Reader(storage).read(manifest_ref))
    expected = {x["path"]: x for x in source["files"] if x["path"] != "lineage.jsonl"}
    validate_baseline(source_manifest["baseline_recheck"], expected)
    refs = [reference(json.loads(line)) for line in Path(input_plan).read_text().splitlines() if line]
    if len(refs) != len(expected) or len({r["id"] for r in refs}) != len(refs):
        raise ValueError("Input plan is not the full source partition")
    for ref in refs:
        if {k: ref[k] for k in ("path", "bytes", "sha256")} != expected[ref["path"]]:
            raise ValueError("Input plan differs from source manifest")
        if ref["uri"] != manifest_ref["uri"].removesuffix("manifest.json") + ref["path"]:
            raise ValueError("Input file outside the source partition")
    event("full_census_started", input_count=len(refs))
    index_path = output / "index"
    sc.parallelize(refs, partitions).mapPartitions(lambda it: index_partition(it, storage)).saveAsPickleFile(str(index_path))
    indexed = sc.pickleFile(str(index_path), minPartitions=partitions)
    census = nested_counts(indexed.mapPartitions(census_partition).reduceByKey(lambda a, b: a + b).collect())
    write_json(output / "census.json", census)
    event("full_census_complete", **census["all"])

    def compare(index, name):
        candidates_path, checked_path = output / (name + "-candidates"), output / (name + "-checks")
        index.filter(lambda r: not r["isolation"]).keyBy(lambda r: r["group"]).groupByKey(partitions).flatMap(group_candidates).map(lambda x: json.dumps(x)).saveAsTextFile(str(candidates_path))
        candidates = sc.textFile(str(candidates_path)).filter(bool).map(json.loads)
        count = candidates.count()
        event(name + "_candidates", count=count)
        candidates.repartition(partitions).mapPartitions(lambda it: check_partition(it, storage)).map(lambda x: json.dumps(x)).saveAsTextFile(str(checked_path))
        checked = sc.textFile(str(checked_path)).filter(bool).map(json.loads)
        stats = dict(checked.map(lambda r: ((r["matched"], r["outside_bound_only"]), 1)).reduceByKey(lambda a, b: a + b).collect())
        if sum(stats.values()) != count:
            raise ValueError("Candidate coverage is incomplete")
        event(name + "_comparison_complete", matched=stats.get((True, False), 0),
              outside_bound_only=stats.get((False, True), 0), candidates=count)
        return checked, stats, count

    checked, stats, pair_count = compare(indexed, "main")
    edges = checked.filter(lambda r: r["matched"]).keyBy(lambda r: r["group"])
    decision_path = output / "decisions"
    indexed.keyBy(lambda r: r["group"]).cogroup(edges, partitions).flatMap(choose_group).map(lambda x: json.dumps(x)).saveAsTextFile(str(decision_path))
    decisions = sc.textFile(str(decision_path)).filter(bool).map(json.loads)
    totals = dict(decisions.map(lambda r: (r["status"], 1)).reduceByKey(lambda a, b: a + b).collect())
    if sum(totals.values()) != len(refs):
        raise ValueError("Decision count mismatch")
    kept = decisions.filter(lambda r: r["status"] == "kept").map(lambda r: r["record"])
    kept_ids = kept.map(lambda r: (r["id"], 1))
    missing = decisions.filter(lambda r: r["status"] == "removed").map(lambda r: (r["retained"]["id"], 1)).subtractByKey(kept_ids)
    if missing.take(1):
        raise ValueError("Removal does not name a final retained representative")
    event("decisions_complete", kept=totals.get("kept", 0), removed=totals.get("removed", 0))
    audit_path = output / "audit"
    decisions.repartition(partitions).mapPartitions(lambda it: audit_partition(it, storage)).map(lambda x: json.dumps(x)).saveAsTextFile(str(audit_path))
    audited = sc.textFile(str(audit_path)).filter(bool).count()
    if audited != totals.get("removed", 0):
        raise ValueError("Independent audit is incomplete")
    event("independent_audit_complete", witnesses=audited)
    retained_path = output / "retained-input"
    kept.map(lambda x: json.dumps(x)).saveAsTextFile(str(retained_path))
    event("retained_recheck_started", count=totals.get("kept", 0))
    reindex_path = output / "recheck-index"
    sc.textFile(str(retained_path)).filter(bool).mapPartitions(lambda it: index_partition(it, storage)).saveAsPickleFile(str(reindex_path))
    reindexed = sc.pickleFile(str(reindex_path), minPartitions=partitions)
    if reindexed.count() != totals.get("kept", 0):
        raise ValueError("Retained index is incomplete")
    _, restats, recount = compare(reindexed, "recheck")
    if restats.get((True, False), 0):
        raise ValueError("Retained relaxed recheck found additional merges")
    Reader(storage).read(manifest_ref)
    models = nested_counts(decisions.map(lambda r: ((r["model"], r["status"]), 1)).reduceByKey(lambda a, b: a + b).collect())
    samples = decisions.filter(lambda r: r["status"] == "removed").takeOrdered(20, key=lambda r: r["record"]["id"])
    write_json(output / "example-references.json", samples)
    report = {"passed": True, "input_count": len(refs), "kept_count": totals.get("kept", 0),
              "removed_count": totals.get("removed", 0), "candidate_pairs": pair_count,
              "matched_pairs": stats.get((True, False), 0),
              "outside_bound_only_pairs": stats.get((False, True), 0), "models": models,
              "census": census, "policy": asdict(POLICY), "mask_version": VERSION,
              "max_placeholder_chars": MAX_SPAN, "literal_templates": sorted(TEMPLATES),
              "source_manifest_sha256": manifest_ref["sha256"],
              "input_plan_sha256": file_sha256(Path(input_plan)),
              "verification": {"source_hashes": len(refs), "removed_witnesses": audited,
                               "retained_recheck_hashes": totals.get("kept", 0),
                               "retained_recheck_candidates": recount, "retained_recheck_matches": 0,
                               "source_manifest_unchanged": True},
              "spark_master": sc.master, "spark_version": spark.version, "s3_writes": False,
              "elapsed_seconds": round(time.monotonic() - started, 2)}
    write_json(output / "report.json", report)
    (output / "SUCCEEDED").write_text("Full relaxed census, decisions, audit and recheck passed.\n")
    event("completed", input=len(refs), kept=report["kept_count"], removed=report["removed_count"])
    return report
