"""Review nearby retained prefixes without relaxing the deletion policy."""

import hashlib
import json
from collections import Counter
from pathlib import Path

from .projection import canonical, covers, project
from .storage import Reader, StorageConfig, atomic_bytes, output_relative_path, reference


def difference(short, long, common):
    fields = []
    a, b = json.loads(short.guard), json.loads(long.guard)
    for key in sorted(a.keys() | b.keys()):
        if canonical(a.get(key)) != canonical(b.get(key)):
            fields.append(key)
    missing = {
        field: sum((Counter(values) - Counter(long.assets[field])).values())
        for field, values in short.assets.items()
    }
    missing = {k: v for k, v in missing.items() if v}
    result = {
        "short_messages": len(short.tokens),
        "long_messages": len(long.tokens),
        "common_messages": common,
        "guard_differences": fields,
        "missing_assets_in_long": missing,
        "isolation": [p.isolation_reason for p in (short, long) if p.isolation_reason],
    }
    if common == len(short.tokens):
        result["category"] = "covered_messages_with_protected_information"
    else:
        a, b = json.loads(short.tokens[common]), json.loads(long.tokens[common])
        keys = [
            k for k in sorted(a.keys() | b.keys()) if canonical(a.get(k)) != canonical(b.get(k))
        ]
        result.update(
            category=str(a["role"]) + "->" + str(b["role"]) + ":" + ",".join(keys),
            different_fields=keys,
            short_message=a,
            long_message=b,
        )
        # Evidence excerpts remain outside Git and outside trajectory records.
        for field in ("short_message", "long_message"):
            result[field] = {
                k: (v[:1200] if isinstance(v, str) else json.dumps(v, ensure_ascii=False)[:1200])
                for k, v in result[field].items()
            }
    return result


def review_files(paths, source, output, policy, layout="flat"):
    reader = Reader(StorageConfig())
    counts = Counter()
    for path in paths:
        previous = None
        samples = []
        sampled = Counter()
        with Path(path).open() as handle:
            for line in handle:
                row = json.loads(line)
                if row["status"] != "kept":
                    continue
                ref = reference(row["record"])
                local = dict(ref, local_path=str(Path(source) / output_relative_path(ref, layout)))
                local.pop("uri", None)
                p = project(reader.node(local), ref["dataset"], ref["date"], policy)
                for group in ("all", ref["dataset"]):
                    counts[(group, "retained_reviewed")] += 1
                if previous is not None and previous[1].scope == p.scope:
                    before, prior = previous
                    left, right, ar, br = (
                        (prior, p, before, ref)
                        if len(prior.tokens) <= len(p.tokens)
                        else (p, prior, ref, before)
                    )
                    common = 0
                    for x, y in zip(left.tokens, right.tokens):
                        if x != y:
                            break
                        common += 1
                    if len(left.tokens) >= 4 and common * 5 >= len(left.tokens) * 4:
                        if covers(left, right) or covers(right, left):
                            raise RuntimeError("Mergeable retained neighbors remain: " + ar["id"])
                        detail = difference(left, right, common)
                        category = detail["category"]
                        for group in ("all", ref["dataset"]):
                            counts[(group, "near_neighbor_pairs")] += 1
                            counts[(group, "category:" + category)] += 1
                        if sampled[category] < 5:
                            samples.append(dict(detail, short=ar, long=br))
                            sampled[category] += 1
                previous = ref, p
        data = (json.dumps(samples, ensure_ascii=False, indent=2) + "\n").encode()
        target = Path(output) / "near-prefix-samples" / (Path(path).name + ".json")
        atomic_bytes(target, data, hashlib.sha256(data).hexdigest())
    yield from counts.items()
