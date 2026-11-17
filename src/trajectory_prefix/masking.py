"""Opt-in, bounded placeholder matching for a shared-disk evaluation only."""

from __future__ import annotations

import re
from collections import Counter
from contextvars import ContextVar

from .projection import canonical, digest

# These four literals were present unchanged in the paired Boyue raw capture.
TEMPLATES = frozenset({"PACK_PATH", "SCHEMA_PATH", "KEY", "PATH"})
LABEL = r"(?:[A-Z][A-Z0-9_]{1,63}|(?i:MASKED|REDACTED|SENSITIVE)(?:_[A-Za-z0-9_]+)?)"
MARKER = re.compile(r"<" + LABEL + r">|\[" + LABEL + r"\]")
MAX_SPAN = 256
VERSION = "placeholder-relaxed-v1"
# Used only by the explanatory counterfactual probe; the actual matcher defaults
# to all approved marker forms. Context-local state cannot leak across workers.
DISABLED_LABELS = ContextVar("disabled_mask_labels", default=frozenset())


def markers(value, *, include_templates=False):
    return [m for m in MARKER.finditer(value)
            if include_templates or (m.group()[1:-1].upper() not in TEMPLATES
                                     and m.group() not in DISABLED_LABELS.get())]


def parts(value):
    result, start = [], 0
    for match in markers(value):
        result.append(value[start:match.start()])
        start = match.end()
    return [*result, value[start:]]


def _append(ranges, low, high):
    if low > high:
        return
    if ranges and low <= ranges[-1][1] + 1:
        ranges[-1] = ranges[-1][0], max(high, ranges[-1][1])
    else:
        ranges.append((low, high))


def _advance(ranges, value, limit):
    result = []
    for low, high in ranges:
        start, bound = low, min(len(value), high + limit)
        while start <= high and start < len(value):
            end = bound
            for newline in ("\r", "\n"):
                pos = value.find(newline, start, bound)
                if pos >= 0:
                    end = min(end, pos)
            if start < end:
                _append(result, start + 1, min(min(high, end - 1) + limit, end))
            if end == bound:
                break
            start = end + 1
    return result


def pattern_matches(pattern, value, limit=MAX_SPAN):
    literals = parts(pattern)
    n = len(literals) - 1
    if not n:
        return pattern == value
    size = sum(map(len, literals))
    if not size + n <= len(value) <= size + limit * n:
        return False
    if not value.startswith(literals[0]) or not value.endswith(literals[-1]):
        return False
    reachable = [(len(literals[0]), len(literals[0]))]
    for index, literal in enumerate(literals[1:], 1):
        consumed = _advance(reachable, value, limit)
        if index == n:
            last = len(value) - len(literal)
            return any(a <= last <= b for a, b in consumed)
        if not literal:
            reachable = consumed
            continue
        reachable = []
        for low, high in consumed:
            end = min(len(value), high + len(literal))
            pos = value.find(literal, low, end)
            while pos >= 0:
                _append(reachable, pos + len(literal), pos + len(literal))
                pos = value.find(literal, pos + 1, end)
        if not reachable:
            return False
    return False


def string_matches(a, b, limit=MAX_SPAN):
    return a == b or pattern_matches(a, b, limit) or pattern_matches(b, a, limit)


def argument_matches(a, b, limit=MAX_SPAN):
    if type(a) is not type(b):
        return False
    if isinstance(a, str):
        return string_matches(a, b, limit)
    if isinstance(a, dict):
        return a.keys() == b.keys() and all(argument_matches(a[k], b[k], limit) for k in a)
    if isinstance(a, list):
        return len(a) == len(b) and all(argument_matches(x, y, limit) for x, y in zip(a, b))
    return canonical(a) == canonical(b)


def message_matches(a, b, limit=MAX_SPAN):
    if a.keys() != b.keys():
        return False
    for key in a:
        if key == "content":
            if not string_matches(a[key], b[key], limit):
                return False
        elif key == "tool_calls":
            if len(a[key]) != len(b[key]):
                return False
            for x, y in zip(a[key], b[key]):
                if x.keys() != y.keys():
                    return False
                for field in x:
                    if field != "function":
                        if canonical(x[field]) != canonical(y[field]):
                            return False
                    else:
                        xf, yf = x[field], y[field]
                        if xf.keys() != yf.keys():
                            return False
                        for name in xf:
                            if name == "arguments":
                                if not argument_matches(xf[name], yf[name], limit):
                                    return False
                            elif canonical(xf[name]) != canonical(yf[name]):
                                return False
        elif canonical(a[key]) != canonical(b[key]):
            return False
    return True


def payload_strings(value, path=""):
    if isinstance(value, str):
        yield path, value
    elif isinstance(value, dict):
        for k, v in value.items():
            yield from payload_strings(v, path + "/" + str(k))
    elif isinstance(value, list):
        for i, v in enumerate(value):
            yield from payload_strings(v, path + "/" + str(i))


def message_strings(message):
    yield "content", message["content"]
    for i, call in enumerate(message.get("tool_calls") or []):
        function = call.get("function") or {}
        if "arguments" in function:
            yield from payload_strings(function["arguments"], f"tool_calls/{i}/function/arguments")


def census_message(message):
    labels, templates = Counter(), Counter()
    for field, value in message_strings(message):
        kind = "arguments" if field.startswith("tool_calls/") else "content"
        for match in markers(value, include_templates=True):
            target = templates if match.group()[1:-1].upper() in TEMPLATES else labels
            target[(kind, match.group())] += 1
    return labels, templates


def argument_shape(value):
    if isinstance(value, str):
        return ["string"]
    if isinstance(value, dict):
        return ["object", [[k, argument_shape(v)] for k, v in sorted(value.items())]]
    if isinstance(value, list):
        return ["array", [argument_shape(x) for x in value]]
    return [type(value).__name__, value]


def message_shape(message):
    value = {k: v for k, v in message.items() if k not in {"content", "tool_calls"}}
    value["content"] = ["string"]
    if "tool_calls" in message:
        calls = []
        for call in message["tool_calls"]:
            function = dict(call["function"])
            if "arguments" in function:
                function["arguments"] = argument_shape(function["arguments"])
            calls.append({**call, "function": function})
        value["tool_calls"] = calls
    return value


def priority(row):
    return -row["message_count"], row["mask_count"], -row["ref"]["bytes"], row["ref"]["id"]


def string_bound(value):
    found = list(markers(value))
    literal_length = len(value) - sum(m.end() - m.start() for m in found)
    return (len(value), digest(value.encode()), value[:64], value[-64:],
            (value[:found[0].start()][:64] if found else value[:64]),
            (value[found[-1].end():][-64:] if found else value[-64:]),
            literal_length + len(found), literal_length + len(found) * MAX_SPAN,
            bool(found))


def bounds_possible(a, b):
    if a[0] == b[0] and a[1] == b[1]:
        return True

    def direction(pattern, value):
        return (pattern[8] and pattern[6] <= value[0] <= pattern[7]
                and value[2].startswith(pattern[4]) and value[3].endswith(pattern[5]))

    return direction(a, b) or direction(b, a)


def possible_pair(a, b):
    """A necessary, complete filter; no text or prefix-hash anchors required."""
    changed = []
    for i, (x, y) in enumerate(zip(a["hashes"], b["hashes"])):
        if a["shapes"][i] != b["shapes"][i]:
            return None
        if x != y:
            if not (a["mask_flags"][i] or b["mask_flags"][i]):
                return None
            if "string_bounds" in a and "string_bounds" in b:
                aa, bb = a["string_bounds"][i], b["string_bounds"][i]
                if aa.keys() != bb.keys() or any(not bounds_possible(aa[k], bb[k]) for k in aa):
                    return None
            changed.append(i)
    return changed or None


def group_candidates(item):
    _, rows = item
    rows = sorted(rows, key=priority)
    # A selective necessary constraint avoids the all-pairs scan for large
    # identities. Shorter sequences remain eligible at every later position.
    from bisect import bisect_right
    from collections import defaultdict
    from itertools import chain

    lengths = [-r["message_count"] for r in rows]
    shapes, plain, masked = defaultdict(list), defaultdict(list), defaultdict(list)
    for j, row in enumerate(rows):
        for p, shape in enumerate(row["shapes"]):
            shapes[p, shape].append(j)
            if row["mask_flags"][p]:
                masked[p, shape].append(j)
            else:
                plain[p, shape, row["hashes"][p]].append(j)
    for i, a in enumerate(rows):
        if not a["mask_count"]:
            continue
        best, options = len(rows) + 1, None
        for p, shape in enumerate(a["shapes"]):
            end = bisect_right(lengths, -(p + 1))
            buckets = ([shapes[p, shape]] if a["mask_flags"][p] else
                       [plain[p, shape, a["hashes"][p]], masked[p, shape]])
            count = sum(map(len, buckets)) + len(rows) - end
            if count < best:
                best, options = count, (*buckets, range(end, len(rows)))
        for j in chain.from_iterable(options or []):
            b = rows[j]
            if i == j or (b["mask_count"] and j < i):
                continue
            changed = possible_pair(a, b)
            if changed:
                long, short = (a, b) if i < j else (b, a)
                yield {"short": short["ref"], "long": long["ref"],
                       "group": short["group"], "different_messages": changed}


def choose_group(item):
    _, (indexed, edges) = item
    rows = sorted(indexed, key=priority)
    order = {row["ref"]["id"]: i for i, row in enumerate(rows)}
    refs = {row["ref"]["id"]: row["ref"] for row in rows}
    targets = {}
    for edge in edges:
        short, long = edge["short"]["id"], edge["long"]["id"]
        if order[long] >= order[short]:
            raise ValueError("Relaxed edge violates representative ordering")
        targets.setdefault(short, set()).add(long)
    kept = set()
    for row in rows:
        key = row["ref"]["id"]
        witnesses = targets.get(key, set()) & kept
        if witnesses:
            final = min(witnesses, key=order.__getitem__)
            yield {"status": "removed", "record": row["ref"], "retained": refs[final],
                   "reason": "placeholder_relaxed", "model": row["model"]}
        else:
            kept.add(key)
            yield {"status": "kept", "record": row["ref"], "model": row["model"],
                   "isolation_reason": row["isolation"]}
