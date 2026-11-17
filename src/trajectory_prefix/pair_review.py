"""Exhaustive small-corpus diagnostics. Candidate transforms never authorize deletion."""

import hashlib
import json
import re
from collections import Counter
from copy import deepcopy
from dataclasses import asdict
from decimal import Decimal
from difflib import SequenceMatcher
from itertools import combinations
from pathlib import Path

from .projection import canonical, covers, project
from .storage import Reader, StorageConfig, file_sha256, reference
from .verification import raw_covers

MASK = re.compile(
    r"<(?:PERSON|LOCATION|ID_NUMBER|EMAIL|PHONE|PHONE_NUMBER|ADDRESS|IP_ADDRESS|ORGANIZATION)>"
)
MODES = ("crlf", "trailing_ascii", "argument_json", "without_headers", "without_notifications",
         "without_headers_and_notifications")


def reject_duplicates(items):
    result = {}
    for key, value in items:
        if key in result:
            raise ValueError("Duplicate JSON key")
        result[key] = value
    return result


def reject_constant(value):
    raise ValueError("Non-standard JSON constant: " + value)


def typed_json(value):
    if isinstance(value, dict):
        return ["object", [[k, typed_json(v)] for k, v in sorted(value.items())]]
    if isinstance(value, list):
        return ["array", [typed_json(v) for v in value]]
    if isinstance(value, Decimal):
        return ["decimal", str(value)]
    return [type(value).__name__, value]


def argument_view(value):
    if not isinstance(value, str):
        return typed_json(value)
    try:
        return typed_json(json.loads(value, parse_float=Decimal,
                                     object_pairs_hook=reject_duplicates,
                                     parse_constant=reject_constant))
    except (ValueError, TypeError):
        return ["unparsed", value]


def transform(messages, mode):
    if mode == "without_headers_and_notifications":
        return transform(transform(messages, "without_headers"), "without_notifications")
    result = deepcopy(messages)
    if mode == "without_headers":
        while result and result[0]["role"] in {"system", "developer"}:
            result.pop(0)
    elif mode == "without_notifications":
        result = [m for m in result if not (
            m["role"] == "user" and m["content"].startswith("<task-notification>")
        )]
    else:
        for message in result:
            if mode == "crlf":
                message["content"] = message["content"].replace("\r\n", "\n")
            elif mode == "trailing_ascii":
                message["content"] = message["content"].rstrip(" \t\r\n")
            elif mode == "argument_json":
                for call in message.get("tool_calls") or []:
                    function = call.get("function") or {}
                    if "arguments" in function:
                        function["arguments"] = argument_view(function["arguments"])
    return result


def _append_range(ranges, low, high):
    if low > high:
        return
    if ranges and low <= ranges[-1][1] + 1:
        ranges[-1] = (ranges[-1][0], max(high, ranges[-1][1]))
    else:
        ranges.append((low, high))


def _consume_mask(ranges, value):
    """Reachable offsets after consuming 1..256 characters other than CR/LF."""
    result = []
    for low, high in ranges:
        start = low
        bound = min(len(value), high + 256)
        while start <= high and start < len(value):
            line_end = bound
            for newline in ("\r", "\n"):
                position = value.find(newline, start, bound)
                if position >= 0:
                    line_end = min(line_end, position)
            if start < line_end:
                last_start = min(high, line_end - 1)
                _append_range(result, start + 1, min(last_start + 256, line_end))
            if line_end == bound:
                break
            start = line_end + 1
    return result


def _masked_pattern_matches(pattern, value):
    parts = MASK.split(pattern)
    masks = len(parts) - 1
    if not masks:
        return False
    literal_length = sum(map(len, parts))
    if not literal_length + masks <= len(value) <= literal_length + 256 * masks:
        return False
    if not value.startswith(parts[0]) or not value.endswith(parts[-1]):
        return False
    reachable = [(len(parts[0]), len(parts[0]))]
    for index, literal in enumerate(parts[1:], 1):
        consumed = _consume_mask(reachable, value)
        if not consumed:
            return False
        if index == masks:
            suffix_start = len(value) - len(literal)
            return any(low <= suffix_start <= high for low, high in consumed)
        if not literal:
            reachable = consumed
            continue
        reachable = []
        for low, high in consumed:
            bound = min(len(value), high + len(literal))
            position = value.find(literal, low, bound)
            while position >= 0:
                end = position + len(literal)
                _append_range(reachable, end, end)
                position = value.find(literal, position + 1, bound)
        if not reachable:
            return False
    return False


def masked_string_candidate(a, b):
    # Restricted and explicitly diagnostic: a placeholder is not proof of its
    # original value. Never pass this relation to the deletion engine.
    # Track reachable offsets instead of regex backtracking: repeated masks in
    # long tool output otherwise cause exponential work on a late mismatch.
    return a == b or _masked_pattern_matches(a, b) or _masked_pattern_matches(b, a)


def masked_candidate(a, b):
    if type(a) is not type(b):
        return False
    if isinstance(a, dict):
        return a.keys() == b.keys() and all(masked_candidate(a[k], b[k]) for k in a)
    if isinstance(a, list):
        return len(a) == len(b) and all(masked_candidate(x, y) for x, y in zip(a, b))
    if isinstance(a, str):
        return masked_string_candidate(a, b)
    return canonical(a) == canonical(b)


def prefix(a, b, comparator=lambda x, y: canonical(x) == canonical(y)):
    return bool(a) and len(a) <= len(b) and all(comparator(x, y) for x, y in zip(a, b))


def fields_differ(a, b, path=""):
    if canonical(a) == canonical(b):
        return []
    if isinstance(a, dict) and isinstance(b, dict):
        result = []
        for key in sorted(a.keys() | b.keys()):
            location = path + "/" + key
            if key not in a or key not in b:
                result.append({"field": location, "missing_side": "a" if key not in a else "b"})
            else:
                result.extend(fields_differ(a[key], b[key], location))
        return result
    if isinstance(a, list) and isinstance(b, list) and len(a) == len(b):
        return [d for i, (x, y) in enumerate(zip(a, b))
                for d in fields_differ(x, y, path + "/" + str(i))]
    sa, sb = a if isinstance(a, str) else json.dumps(a), b if isinstance(b, str) else json.dumps(b)
    common = 0
    while common < min(len(sa), len(sb)) and sa[common] == sb[common]:
        common += 1
    return [{"field": path, "lengths": [len(sa), len(sb)], "first_different_char": common,
             "a_excerpt": sa[max(0, common - 80):common + 320],
             "b_excerpt": sb[max(0, common - 80):common + 320],
             "redaction_candidate": masked_candidate(a, b)}]


def review_pair(a, b, ar, br, policy):
    ap, bp = project(a, ar["dataset"], ar["date"], policy), project(b, br["dataset"], br["date"], policy)
    am, bm = [json.loads(t) for t in ap.tokens], [json.loads(t) for t in bp.tokens]
    common = 0
    for x, y in zip(ap.tokens, bp.tokens):
        if x != y:
            break
        common += 1
    current = [covers(ap, bp), covers(bp, ap)]
    independent = [raw_covers(a, b, ar, br, policy), raw_covers(b, a, br, ar, policy)]
    if current != independent:
        raise RuntimeError("Independent current-policy comparator disagrees")
    sa, sb = json.loads(ap.scope), json.loads(bp.scope)
    scope_names = ["dataset", "model", "harness", "user", "end_user", "identity_kind", "session_or_date"]
    row = {"a": ar, "b": br, "same_scope": sa == sb,
           "scope_differences": [name for name, x, y in zip(scope_names, sa, sb) if x != y],
           "messages": [len(am), len(bm)], "common_messages": common,
           "raw_messages": [len(a["messages"]), len(b["messages"])],
           "current_covers": current, "independent_current_covers": independent,
           "isolation": [ap.isolation_reason, bp.isolation_reason],
           "message_prefix_without_scope": [prefix(am, bm), prefix(bm, am)]}
    if common < min(len(am), len(bm)):
        row["first_difference"] = {"roles": [am[common]["role"], bm[common]["role"]],
                                   "details": fields_differ(am[common], bm[common])}
    variants = {}
    transformed = {}
    for mode in MODES:
        av, bv = transform(am, mode), transform(bm, mode)
        transformed[mode] = (av, bv)
        variants[mode] = [prefix(av, bv), prefix(bv, av)]
    variants["redaction_placeholder_candidates"] = [
        prefix(am, bm, masked_candidate), prefix(bm, am, masked_candidate)
    ]
    for mode in ("without_notifications", "without_headers_and_notifications"):
        av, bv = transformed[mode]
        variants["redaction_" + mode] = [
            prefix(av, bv, masked_candidate), prefix(bv, av, masked_candidate)
        ]
    row["diagnostic_prefix_candidates"] = {k:v for k,v in variants.items() if any(v)}
    if sa == sb:
        ah, bh = [[hashlib.sha256(t).digest() for t in p.tokens] for p in (ap, bp)]
        matcher = SequenceMatcher(None, ah, bh, autojunk=False)
        opcodes = matcher.get_opcodes()
        row["matching_messages"] = sum(i2-i1 for tag, i1, i2, _, _ in opcodes if tag == "equal")
        row["alignment"] = [{"kind":tag,"a_range":[i1,i2],"b_range":[j1,j2]}
                            for tag,i1,i2,j1,j2 in opcodes if tag != "equal"]
        limit = min(len(am), len(bm))
        row["position_differences"] = [
            {"index":i,"roles":[am[i]["role"],bm[i]["role"]],
             "details":fields_differ(am[i],bm[i])}
            for i in range(limit) if ap.tokens[i] != bp.tokens[i]
        ]
    return row


def pair_partition(pairs, policy):
    reader = Reader(StorageConfig())
    cache = {}
    for ar, br in pairs:
        for ref in (ar, br):
            if ref["id"] not in cache:
                # The bounded corpus is at most 256 MiB; each executor partition
                # reads original bytes, validates the hash, and reuses only nodes.
                local = dict(ref)
                local.pop("uri", None)
                cache[ref["id"]] = reader.node(local)
        yield json.dumps(review_pair(cache[ar["id"]], cache[br["id"]], ar, br, policy),
                         ensure_ascii=False, separators=(",", ":"))


def run(spark, input_plan, output, *, partitions, policy):
    if policy.extra_information != "messages_only":
        raise ValueError("Pair diagnostics currently require messages_only")
    refs = [reference(json.loads(line)) for line in input_plan.read_text().splitlines() if line.strip()]
    if not 2 <= len(refs) <= 128 or sum(r["bytes"] for r in refs) > 256 * 1024 * 1024:
        raise ValueError("Exhaustive review is limited to 2-128 full records totaling <=256 MiB")
    if len({r["id"] for r in refs}) != len(refs) or any(not r.get("local_path") for r in refs):
        raise ValueError("Unique published local references are required")
    output = Path(output).resolve()
    if not spark.sparkContext.master.startswith("local") and str(output).startswith(("/tmp/", "/home/")):
        raise ValueError("Cluster review requires a shared output directory")
    output.mkdir(parents=True, exist_ok=False)
    pairs = list(combinations(refs, 2))
    spark.sparkContext.parallelize(pairs, min(partitions, len(pairs))).mapPartitions(
        lambda rows: pair_partition(rows, policy)
    ).saveAsTextFile(str(output / "pairs"))
    counts = Counter()
    actual = 0
    for part in (output / "pairs").glob("part-*"):
        for line in part.read_text().splitlines():
            row = json.loads(line)
            actual += 1
            scope = "same_scope" if row["same_scope"] else "different_scope"
            counts[scope] += 1
            counts["current_mergeable_pairs"] += int(any(row["current_covers"]))
            for mode in row["diagnostic_prefix_candidates"]:
                counts[scope + ":candidate:" + mode] += 1
    if actual != len(pairs):
        raise RuntimeError("Incomplete pair review")
    report = {"spark_master":spark.sparkContext.master, "spark_version":spark.version,
              "input_count":len(refs), "pairs_compared":actual, "counts":dict(counts),
              "policy":asdict(policy), "input_plan_sha256":file_sha256(input_plan),
              "diagnostic_only":True,
              "limitation":"Transforms are candidate probes, not deletion rules. Mask placeholders and omitted notifications do not prove equivalent original information. All pairs were examined; input trajectories were not changed."}
    (output / "report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    (output / "SUCCEEDED").write_text("all bounded input pairs examined\n")
    return report
