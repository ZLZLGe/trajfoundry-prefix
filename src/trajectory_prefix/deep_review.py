"""Scalable retained-prefix diagnostics. No transformation here authorizes deletion."""

from __future__ import annotations

import json
import re
from collections import Counter, OrderedDict
from dataclasses import asdict
from pathlib import Path

import orjson

from .pair_review import fields_differ, masked_candidate, prefix, transform
from .projection import Policy, canonical, digest, project
from .storage import Reader, file_sha256, reference
from .verification import raw_covers

MODES = (
    "current", "format", "without_headers", "without_notifications",
    "without_headers_and_notifications", "format_without_headers_and_notifications",
    "cross_identity",
)
MASK_BYTES = re.compile(
    rb"<(?:PERSON|LOCATION|ID_NUMBER|EMAIL|PHONE|PHONE_NUMBER|ADDRESS|IP_ADDRESS|ORGANIZATION)>"
)


def variant(messages, mode):
    if mode in {"current", "cross_identity"}:
        return messages
    if mode in {"format", "format_without_headers_and_notifications"}:
        value = messages
        for name in ("crlf", "trailing_ascii", "argument_json"):
            value = transform(value, name)
        if mode == "format_without_headers_and_notifications":
            value = transform(value, "without_headers_and_notifications")
        return value
    return transform(messages, mode)


def scope_for(ref, projection, mode):
    if mode != "cross_identity":
        return digest(projection.scope)
    scope = json.loads(projection.scope)
    # A diagnostic of identity boundaries, still separated by dataset/model/harness/date.
    return digest(canonical([scope[0], scope[1], scope[2], ref["date"]]))


def make_index(ref, node, policy):
    p = project(node, ref["dataset"], ref["date"], policy)
    messages = [orjson.loads(t) for t in p.tokens]
    scope = json.loads(p.scope)
    variants, pool = {}, {}
    for mode in MODES:
        if not p.mergeable:
            break
        values = variant(messages, mode)
        minimum = 2 if mode in {"current", "format"} else 4
        if mode == "cross_identity":
            minimum = 6
            if not any(m["role"] == "tool" for m in messages):
                continue
            if sum(len(m["content"]) for m in messages) < 1024:
                continue
        if len(values) < minimum:
            continue
        tokens = [canonical(m) for m in values]
        vector = b"".join(digest(t) for t in tokens)
        masks = "".join("1" if MASK_BYTES.search(t) else "0" for t in tokens)
        roles = "".join({"system": "S", "developer": "D", "user": "U",
                         "assistant": "A", "tool": "T"}[m["role"]] for m in values)
        # Pickle preserves sharing between modes that make no actual change.
        pair = pool.setdefault((vector, roles, masks), (vector, roles, masks))
        variants[mode] = pair
    return {
        "ref": ref, "scope": p.scope, "scope_hash": digest(p.scope),
        "cross_scope": scope_for(ref, p, "cross_identity"),
        "model": str(node.get("model", "")), "harness": str(node.get("harness", "")),
        "sessionless": scope[-2] == "sessionless", "isolation": p.isolation_reason,
        "message_count": len(messages), "variants": variants,
    }


def index_partition(lines, policy, storage):
    reader = Reader(storage)
    for line in lines:
        ref = reference(json.loads(line))
        yield make_index(ref, reader.node(ref), policy)


def count_index(rows):
    counts = Counter()
    for row in rows:
        for group in ("all", row["ref"]["dataset"],
                      row["ref"]["dataset"] + "/model=" + row["model"]):
            counts[(group, "input")] += 1
            counts[(group, "sessionless" if row["sessionless"] else "with_session")] += 1
            if row["isolation"]:
                counts[(group, "isolated:" + row["isolation"])] += 1
            for mode in row["variants"]:
                counts[(group, "eligible:" + mode)] += 1
    yield from counts.items()


def sort_entries(row):
    for mode, (vector, roles, masks) in row["variants"].items():
        scope = row["cross_scope"] if mode == "cross_identity" else row["scope_hash"]
        ref = row["ref"]
        yield (mode, scope, vector, -ref["bytes"], ref["id"]), {
            "ref": ref, "roles": roles, "masks": masks, "scope": row["scope_hash"],
        }


def partition_key(key):
    return int.from_bytes(digest(key[0].encode() + key[1] + key[2][:64])[:8], "big")


def scan_candidates(rows):
    previous = None
    for key, value in rows:
        mode, scope, vector = key[:3]
        if previous is not None and previous[0][:2] == key[:2]:
            prior_key, prior = previous
            old_vector = prior_key[2]
            if old_vector == vector:
                yield {"kind": "coverage_candidate", "relation": "equal", "mode": mode,
                       "short": value["ref"], "long": prior["ref"],
                       "same_original_scope": value["scope"] == prior["scope"]}
                # Keep the first representative until this equal-vector group ends.
                continue
            common = 0
            limit = min(len(old_vector), len(vector))
            while common < limit and old_vector[common:common + 32] == vector[common:common + 32]:
                common += 32
            if common == len(old_vector):
                yield {"kind": "coverage_candidate", "relation": "prefix", "mode": mode,
                       "short": prior["ref"], "long": value["ref"],
                       "same_original_scope": value["scope"] == prior["scope"]}
            elif mode != "cross_identity" and limit >= 6 * 32 and common < limit:
                position = common // 32
                masked = (prior.get("masks", "0" * len(prior["roles"]))[position] == "1"
                          or value.get("masks", "0" * len(value["roles"]))[position] == "1")
                ordinary_near = common >= 4 * 32 and common * 2 >= limit
                masked_near = masked and common >= 2 * 32
                if not (ordinary_near or masked_near):
                    previous = key, value
                    continue
                a, b = (prior, value) if len(old_vector) <= len(vector) else (value, prior)
                yield {"kind": "near_candidate", "mode": mode,
                       "short": a["ref"], "long": b["ref"],
                       "common_messages": position, "short_messages": limit // 32,
                       "long_messages": max(len(old_vector), len(vector)) // 32,
                       "first_difference_roles": a["roles"][position] + b["roles"][position],
                       "first_difference_masked": masked,
                       "same_original_scope": a["scope"] == b["scope"]}
        previous = key, value


class NodeReader:
    def __init__(self, storage):
        self.reader = Reader(storage)
        self.cache = OrderedDict()
        self.size = 0

    def node(self, ref):
        key = ref["sha256"]
        if key in self.cache:
            data = self.cache.pop(key)
            self.cache[key] = data
        else:
            data = self.reader.read(ref)
            self.cache[key] = data
            self.size += len(data)
            while self.size > 64 * 1024 * 1024 and len(self.cache) > 1:
                self.size -= len(self.cache.popitem(last=False)[1])
        return orjson.loads(data)


def verify_candidate(row, short, long, policy):
    ar, br = row["short"], row["long"]
    ap, bp = project(short, ar["dataset"], ar["date"], policy), project(
        long, br["dataset"], br["date"], policy
    )
    am, bm = [orjson.loads(t) for t in ap.tokens], [orjson.loads(t) for t in bp.tokens]
    mode = row["mode"]
    av, bv = variant(am, mode), variant(bm, mode)
    if (
        not ap.mergeable or not bp.mergeable
        or scope_for(ar, ap, mode) != scope_for(br, bp, mode)
        or not prefix(av, bv)
    ):
        raise RuntimeError("Diagnostic candidate did not verify against full original JSON")
    original_scope_equal = ap.scope == bp.scope
    if original_scope_equal != row["same_original_scope"]:
        raise RuntimeError("Diagnostic identity metadata changed")
    result = dict(row)
    result.update(
        verified_full_transformed_prefix=True,
        current_rule_covers=raw_covers(short, long, ar, br, policy),
        model=short.get("model", ""), harness=short.get("harness", ""),
        original_messages=[len(am), len(bm)], transformed_messages=[len(av), len(bv)],
        individual_transforms={
            name: prefix(transform(am, name), transform(bm, name))
            for name in ("crlf", "trailing_ascii", "argument_json", "without_headers",
                         "without_notifications", "without_headers_and_notifications")
        },
    )
    names = ["dataset", "model", "harness", "user", "end_user", "identity_kind", "session_or_date"]
    result["scope_differences"] = [
        name for name, a, b in zip(names, json.loads(ap.scope), json.loads(bp.scope)) if a != b
    ]
    return result


def verify_partition(rows, policy, storage):
    reader = NodeReader(storage)
    for row in rows:
        yield verify_candidate(row, reader.node(row["short"]), reader.node(row["long"]), policy)


def verify_mask_partition(rows, policy, storage):
    reader = NodeReader(storage)
    for row in rows:
        a, b = reader.node(row["short"]), reader.node(row["long"])
        ap = project(a, row["short"]["dataset"], row["short"]["date"], policy)
        bp = project(b, row["long"]["dataset"], row["long"]["date"], policy)
        if ap.scope != bp.scope:
            raise RuntimeError("Masked-neighbor identity scope changed")
        av = variant([orjson.loads(t) for t in ap.tokens], row["mode"])
        bv = variant([orjson.loads(t) for t in bp.tokens], row["mode"])
        yield dict(row, full_sequences_checked=True, diagnostic_only=True,
                   mask_prefix_directions=[prefix(av, bv, masked_candidate),
                                           prefix(bv, av, masked_candidate)],
                   current_rule_covers=raw_covers(a, b, row["short"], row["long"], policy))


def count_masks(rows):
    counts = Counter()
    for row in rows:
        for group in ("all", row["short"]["dataset"]):
            counts[(group, row["mode"], "full_pairs_checked")] += 1
            counts[(group, row["mode"], "mask_prefix_candidate_pairs")] += int(
                any(row["mask_prefix_directions"])
            )
            counts[(group, row["mode"], "current_rule_covers")] += int(row["current_rule_covers"])
    yield from counts.items()


def sample_key(row):
    return digest(canonical([row["mode"], row["short"]["id"], row["long"]["id"]])).hex()


def sample_group(row):
    return (row["short"]["dataset"], row["mode"],
            row.get("first_difference_roles", row.get("relation", "")))


def bounded_merge(a, b):
    unique = {sample_key(row): row for row in a + b}
    return [unique[k] for k in sorted(unique)[:5]]


def explain_partition(rows, policy, storage):
    reader = NodeReader(storage)
    for row in rows:
        a, b = reader.node(row["short"]), reader.node(row["long"])
        ap = project(a, row["short"]["dataset"], row["short"]["date"], policy)
        bp = project(b, row["long"]["dataset"], row["long"]["date"], policy)
        am, bm = [orjson.loads(t) for t in ap.tokens], [orjson.loads(t) for t in bp.tokens]
        common = 0
        for x, y in zip(ap.tokens, bp.tokens):
            if x != y:
                break
            common += 1
        detail = dict(row, common_original_messages=common,
                      original_messages=[len(am), len(bm)],
                      current_rule_covers=[raw_covers(a, b, row["short"], row["long"], policy),
                                           raw_covers(b, a, row["long"], row["short"], policy)])
        if common < min(len(am), len(bm)):
            detail["first_difference"] = {
                "roles": [am[common]["role"], bm[common]["role"]],
                "fields": fields_differ(am[common], bm[common]),
            }
        # These checks apply to the entire shorter sequence, not just the first difference.
        probes = {}
        for mode in MODES:
            if mode == "cross_identity":
                continue
            av, bv = variant(am, mode), variant(bm, mode)
            probes[mode] = [prefix(av, bv), prefix(bv, av)]
        for mode in ("current", "without_notifications", "without_headers_and_notifications"):
            av, bv = variant(am, mode), variant(bm, mode)
            probes["mask_candidate:" + mode] = [
                prefix(av, bv, masked_candidate), prefix(bv, av, masked_candidate)
            ]
        detail["full_sequence_probe_results"] = probes
        detail["diagnostic_only"] = True
        yield detail


def counted(rows, verified=False):
    counts = Counter()
    for row in rows:
        mode = row["mode"]
        for group in ("all", row["short"]["dataset"]):
            counts[(group, mode, row["kind"])] += 1
            if row["kind"] == "near_candidate":
                counts[(group, mode, "near_roles:" + row["first_difference_roles"])] += 1
                counts[(group, mode, "first_difference_masked")] += int(row["first_difference_masked"])
            if verified:
                counts[(group, mode, "current_rule_covers")] += int(row["current_rule_covers"])
                counts[(group, mode, "different_identity")] += int(not row["same_original_scope"])
                for name, passed in row["individual_transforms"].items():
                    counts[(group, mode, "also_matches:" + name)] += int(passed)
    yield from counts.items()


def nested(counts):
    result = {}
    for (group, mode, metric), count in counts.items():
        result.setdefault(group, {}).setdefault(mode, {})[metric] = count
    return result


def run(spark, input_plan, output, *, partitions, storage, expected_count, expected_sha):
    from pyspark import StorageLevel

    if not spark.sparkContext.master.startswith("k8s://"):
        raise RuntimeError("Full retained review requires Spark Cluster")
    output = Path(output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    sc = spark.sparkContext
    policy = Policy(extra_information="messages_only")
    if file_sha256(Path(input_plan)) != expected_sha:
        raise RuntimeError("Retained input selection changed")

    def event(stage, **extra):
        row = dict(stage=stage, **extra)
        with (output / "progress.jsonl").open("a") as handle:
            handle.write(json.dumps(row) + "\n")
        print(json.dumps(row), flush=True)

    event("index_started", input_count=expected_count)
    inputs = sc.textFile(str(input_plan), minPartitions=partitions).filter(lambda s: bool(s.strip()))
    inputs.mapPartitions(lambda rows: index_partition(rows, policy, storage)).saveAsPickleFile(
        str(output / "index")
    )
    index = sc.pickleFile(str(output / "index"), minPartitions=partitions)
    totals = dict(index.mapPartitions(count_index).reduceByKey(lambda a, b: a + b).collect())
    if totals.get(("all", "input")) != expected_count:
        raise RuntimeError("Incomplete retained index")
    inventory = {group: {k: n for (g, k), n in totals.items() if g == group}
                 for group in sorted({g for g, _ in totals})}
    (output / "inventory.json").write_text(json.dumps(inventory, indent=2) + "\n")
    event("index_complete", count=expected_count)
    index.flatMap(sort_entries).repartitionAndSortWithinPartitions(
        partitions, partitionFunc=partition_key
    ).mapPartitions(scan_candidates).map(lambda r: json.dumps(r, separators=(",", ":"))).saveAsTextFile(
        str(output / "candidates")
    )
    candidates = sc.textFile(str(output / "candidates"), minPartitions=partitions).map(json.loads)
    counts = dict(candidates.mapPartitions(counted).reduceByKey(lambda a, b: a + b).collect())
    (output / "candidate-counts.json").write_text(json.dumps(nested(counts), indent=2) + "\n")
    event("candidates_complete", all_counts=nested(counts).get("all", {}))
    coverage = candidates.filter(lambda r: r["kind"] == "coverage_candidate")
    verified = coverage.mapPartitions(lambda rows: verify_partition(rows, policy, storage)).persist(
        StorageLevel.DISK_ONLY
    )
    verified.map(lambda r: json.dumps(r, ensure_ascii=False, separators=(",", ":"))).saveAsTextFile(
        str(output / "verified-candidates")
    )
    vcounts = dict(verified.mapPartitions(lambda rows: counted(rows, True)).reduceByKey(
        lambda a, b: a + b
    ).collect())
    for (group, mode, metric), n in counts.items():
        if metric == "coverage_candidate" and vcounts.get((group, mode, metric)) != n:
            raise RuntimeError("Not every transformed-prefix candidate was verified")
    event("all_coverage_candidates_verified", all_counts=nested(vcounts).get("all", {}))
    masked = candidates.filter(lambda r: r["kind"] == "near_candidate" and r["first_difference_masked"])
    mask_checks = masked.mapPartitions(lambda rows: verify_mask_partition(rows, policy, storage)).persist(
        StorageLevel.DISK_ONLY
    )
    mask_checks.map(lambda r: json.dumps(r, ensure_ascii=False, separators=(",", ":"))).saveAsTextFile(
        str(output / "mask-candidate-checks")
    )
    mask_counts = dict(mask_checks.mapPartitions(count_masks).reduceByKey(lambda a, b: a + b).collect())
    for (group, mode, metric), n in counts.items():
        if metric == "first_difference_masked" and mask_counts.get((group, mode, "full_pairs_checked"), 0) != n:
            raise RuntimeError("Not every masked-neighbor candidate was checked")
    positive_masks = mask_checks.filter(lambda r: any(r["mask_prefix_directions"]))
    mask_unique_pairs = positive_masks.map(
        lambda r: tuple(sorted((r["short"]["id"], r["long"]["id"])))
    ).distinct(partitions).count()
    (output / "mask-counts.json").write_text(json.dumps({
        "counts": nested(mask_counts), "unique_candidate_pairs_across_modes": mask_unique_pairs,
        "diagnostic_only": True,
    }, indent=2) + "\n")
    event("all_masked_neighbor_candidates_checked", unique_candidate_pairs=mask_unique_pairs)
    samples = candidates.map(lambda row: (sample_group(row), [row])).reduceByKey(
        bounded_merge, partitions
    ).flatMap(lambda item: item[1]).collect()
    mask_samples = positive_masks.map(
        lambda row: ((row["short"]["dataset"], row["mode"]), [row])
    ).reduceByKey(bounded_merge, partitions).flatMap(lambda item: item[1]).collect()
    samples = list({sample_key(row): row for row in samples + mask_samples}.values())
    (output / "sample-inputs.json").write_text(json.dumps(samples, ensure_ascii=False, indent=2) + "\n")
    sc.parallelize(samples, min(partitions, len(samples)) or 1).mapPartitions(
        lambda rows: explain_partition(rows, policy, storage)
    ).map(lambda r: json.dumps(r, ensure_ascii=False, separators=(",", ":"))).saveAsTextFile(
        str(output / "sample-evidence")
    )
    if file_sha256(Path(input_plan)) != expected_sha:
        raise RuntimeError("Retained selection changed during review")
    report = {
        "passed": True, "spark_master": sc.master, "spark_version": spark.version,
        "input_plan": str(input_plan), "input_plan_sha256": expected_sha,
        "input_count": expected_count, "policy": asdict(policy), "modes": list(MODES),
        "inventory": inventory, "candidate_counts": nested(counts),
        "verified_candidate_counts": nested(vcounts), "evidence_pairs": len(samples),
        "masked_neighbor_counts": nested(mask_counts),
        "unique_mask_candidate_pairs_across_modes": mask_unique_pairs,
        "diagnostic_only": True, "s3_writes": False,
        "limitations": [
            "Current and format modes require >=2 projected messages; header/notification probes >=4; cross-identity >=6 plus tool result and 1024 content characters.",
            "Every eligible record enters each mode's prefix comparison. Every emitted coverage candidate is checked against full raw JSON, not only hashes.",
            "Mode counts overlap and are candidate relationships, not permission or an estimate to delete that many trajectories.",
            "Near-candidate counts cover sorted adjacent distinct vectors with >=4 shared messages and >=50% of a shorter sequence of >=6 messages, or >=2 shared messages when the first difference contains a known masking placeholder. They do not enumerate every possible pair or first-two-message masking differences.",
            "Every first-difference-masked neighbor is checked over the full transformed sequences. This is complete within the candidate set, not an exhaustive count of all possible masking matches.",
            "Detailed differences use five deterministic examples per dataset/mode/relation-or-role bucket, plus five positive masking examples per dataset/mode. Placeholders do not prove original equality or permission to delete.",
        ],
    }
    (output / "report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    (output / "SUCCEEDED").write_text("full index and all coverage-candidate checks complete\n")
    verified.unpersist()
    mask_checks.unpersist()
    event("deep_review_complete", evidence_pairs=len(samples))
    return report
