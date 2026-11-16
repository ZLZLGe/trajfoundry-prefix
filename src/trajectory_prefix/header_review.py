"""Find retained equal bodies with different leading instructions, without deleting anything."""

import hashlib
import json
from dataclasses import asdict
from pathlib import Path

from .projection import canonical, digest, project
from .spark import finish
from .storage import Reader, StorageConfig, reference


def split_headers(projection):
    count = 0
    for token in projection.tokens:
        if json.loads(token)["role"] not in {"system", "developer"}:
            break
        count += 1
    return projection.tokens[:count], projection.tokens[count:]


def candidate(entry, node, policy):
    p = project(node, entry["dataset"], entry["date"], policy)
    headers, body = split_headers(p)
    if not p.mergeable or len(body) < 4:
        return None
    assets = {
        name: sorted(digest(value).hex() for value in values) for name, values in p.assets.items()
    }
    key = (
        digest(p.scope).hex(),
        digest(p.vector[len(headers) * 32 :]).hex(),
        digest(canonical([p.guard.decode(), assets])).hex(),
    )
    header_hash = digest(b"".join(digest(value) for value in headers)).hex()
    return key, {
        "record_count": 1,
        "body_messages": len(body),
        "variants": [[header_hash, entry]],
    }


def merge_candidates(left, right):
    variants = {}
    for fingerprint, entry in left["variants"] + right["variants"]:
        if fingerprint not in variants or entry["id"] < variants[fingerprint]["id"]:
            variants[fingerprint] = entry
    return {
        "record_count": left["record_count"] + right["record_count"],
        "body_messages": left["body_messages"],
        # Two distinct fingerprints prove variation; keeping the two minimum
        # values makes this bounded reduction associative and deterministic.
        "variants": [[key, variants[key]] for key in sorted(variants)[:2]],
    }


def project_partition(lines, policy):
    reader = Reader(StorageConfig())
    for line in lines:
        entry = reference(json.loads(line))
        if not entry.get("local_path"):
            raise ValueError("Header review requires published local/shared retained inputs")
        entry.pop("uri", None)
        item = candidate(entry, reader.node(entry), policy)
        if item is not None:
            yield item


def normalized_headers(tokens, mode):
    result = []
    for token in tokens:
        item = json.loads(token)
        if mode == "trailing_ascii":
            item["content"] = item["content"].rstrip(" \t\r\n")
        elif mode == "crlf":
            item["content"] = item["content"].replace("\r\n", "\n")
        elif mode == "billing_line" and item["content"].startswith("x-anthropic-billing-header:"):
            _, separator, rest = item["content"].partition("\n")
            item["content"] = rest if separator else ""
        result.append(canonical(item))
    return tuple(result)


def verify_partition(rows, policy):
    reader = Reader(StorageConfig())
    for key, group in rows:
        entries = [entry for _, entry in group["variants"]]
        projections = [project(reader.node(e), e["dataset"], e["date"], policy) for e in entries]
        a, b = projections
        ah, ab = split_headers(a)
        bh, bb = split_headers(b)
        if (
            not a.mergeable
            or not b.mergeable
            or a.scope != b.scope
            or a.guard != b.guard
            or ab != bb
            or ah == bh
            or any(sorted(a.assets[k]) != sorted(b.assets[k]) for k in a.assets)
        ):
            raise RuntimeError("Header candidate did not verify against actual retained bytes")
        flags = {
            mode: normalized_headers(ah, mode) == normalized_headers(bh, mode)
            for mode in ("trailing_ascii", "crlf", "billing_line")
        }
        yield {
            "candidate_key": hashlib.sha256(canonical(key)).hexdigest(),
            "record_count": group["record_count"],
            "body_messages": group["body_messages"],
            "leading_header_counts": [len(ah), len(bh)],
            "verified_equal_body_pair": True,
            "equal_after_header_transform": flags,
            "representatives": entries,
        }


def run(spark, input_plan: Path, output: Path, *, partitions, policy):
    from pyspark import StorageLevel

    output = output.resolve()
    if not spark.sparkContext.master.startswith("local") and str(output).startswith(
        ("/tmp/", "/home/")
    ):
        raise ValueError("Cluster review requires a shared output directory")
    output.mkdir(parents=True, exist_ok=False)
    (output / "RUNNING").write_text("Reviewing leading headers\n")
    sc = spark.sparkContext
    inputs = sc.textFile(str(input_plan.resolve()), minPartitions=partitions).filter(
        lambda s: bool(s.strip())
    )
    input_count = inputs.count()
    projected = inputs.mapPartitions(lambda rows: project_partition(rows, policy)).persist(
        StorageLevel.DISK_ONLY
    )
    verified = None
    try:
        eligible = projected.count()
        groups = projected.reduceByKey(merge_candidates, partitions).filter(
            lambda row: len(row[1]["variants"]) == 2
        )
        verified = groups.mapPartitions(lambda rows: verify_partition(rows, policy)).persist(
            StorageLevel.DISK_ONLY
        )
        verified.map(
            lambda row: json.dumps(row, ensure_ascii=False, separators=(",", ":"))
        ).saveAsTextFile(str(output / "candidates"))
        totals = verified.map(
            lambda row: (
                1,
                row["record_count"],
                int(row["equal_after_header_transform"]["trailing_ascii"]),
                int(row["equal_after_header_transform"]["crlf"]),
                int(row["equal_after_header_transform"]["billing_line"]),
            )
        ).fold((0, 0, 0, 0, 0), lambda a, b: tuple(x + y for x, y in zip(a, b)))
        sample = verified.takeOrdered(300, key=lambda row: row["candidate_key"])
        (output / "sample.json").write_text(json.dumps(sample, ensure_ascii=False, indent=2) + "\n")
        return finish(
            output,
            {
                "spark_master": sc.master,
                "spark_version": spark.version,
                "review_kind": "equal_bodies_with_different_leading_headers",
                "policy": asdict(policy),
                "input_count": input_count,
                "eligible_count": eligible,
                "candidate_groups": totals[0],
                "records_in_candidate_groups": totals[1],
                "representative_pairs_equal_after_transform": dict(
                    zip(("trailing_ascii", "crlf", "billing_line"), totals[2:])
                ),
                "limitation": "At least four body messages. Counts describe candidate groups, not safe deletions. Exactly two distinct-header representatives per group are verified; no trajectory is deleted or copied.",
            },
        )
    finally:
        projected.unpersist()
        if verified is not None:
            verified.unpersist()
