"""Independent raw-object witness verification, without projection hashes or the scan engine."""

from __future__ import annotations

import json
import re
from collections import Counter

from .projection import ASSETS, DERIVED, METADATA_PROVENANCE, PROVENANCE, Policy


def encoded(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _id(value):
    if (
        not isinstance(value, str)
        or value.strip().lower() in {"", "no_session_id", "no_user_id", "null", "none", "unknown"}
        or (value.startswith("<") and value.endswith(">"))
    ):
        return ""
    return value


def _scope(node, dataset, date):
    md = node.get("metadata") or {}
    s = _id(md.get("session_id"))
    return (
        dataset,
        node.get("model", ""),
        node.get("harness", ""),
        _id(md.get("user_id")),
        _id(md.get("end_user_id")),
        "session" if s else "sessionless",
        s or date,
    )


def _eligible(node, policy):
    messages = node["messages"]
    roles = {m["role"] for m in messages}
    if not {"user", "assistant"} <= roles:
        return False
    md = node.get("metadata") or {}
    if not any(_id(md.get(k)) for k in ("session_id", "user_id", "end_user_id")):
        chars = sum(
            len(m["content"]) for m in messages if m["role"] in {"user", "assistant", "tool"}
        )
        if (
            len(messages) < policy.anonymous_min_messages
            or chars < policy.anonymous_min_chars
            or "tool" not in roles
        ):
            return False
    for issue in (node.get("normalization_audit") or {}).get("issues", []):
        if issue.get("code") in {"metadata_session_id_masked", "metadata_user_id_masked"}:
            return False
    if "tool_id_alpha" in policy.rules:
        names = {}
        for m in messages:
            if m["role"] == "assistant":
                for c in m.get("tool_calls") or []:
                    identifier = c.get("id")
                    if not isinstance(identifier, str) or not identifier or identifier in names:
                        return False
                    names[identifier] = c.get("function", {}).get("name", "")
            elif m["role"] == "tool":
                identifier = m.get("tool_call_id")
                if identifier not in names or names[identifier] != m.get("name", ""):
                    return False
    return True


def _header(value, policy):
    if "billing_cch" not in policy.rules or not value.startswith("x-anthropic-billing-header:"):
        return value
    first, separator, rest = value.partition("\n")
    first = re.sub(r"\bcch=[0-9a-fA-F]+;", "cch=<volatile-cch>;", first, count=1)
    return first + separator + rest


def _guard(node, policy):
    if policy.extra_information == "tools_only":
        return {}
    result = {
        k: v for k, v in node.items() if k not in DERIVED | PROVENANCE | ASSETS | {"messages"}
    }
    if isinstance(result.get("instructions"), str):
        result["instructions"] = _header(result["instructions"], policy)
    result["metadata_extra"] = {
        k: v for k, v in (node.get("metadata") or {}).items() if k not in METADATA_PROVENANCE
    }
    result["is_subagent"] = bool((node.get("completeness") or {}).get("is_subagent"))
    return result


def _message(message, policy):
    result = {
        k: v
        for k, v in message.items()
        if k not in {"reasoning_content", "reasoning_details", "reasoning"}
    }
    if result["role"] in {"system", "developer"}:
        result["content"] = _header(result["content"], policy)
    if result["role"] == "assistant":
        if "assistant_trailing_space" in policy.rules:
            result["content"] = result["content"].rstrip(" \t\r\n")
        if not result.get("tool_calls"):
            result.pop("tool_calls", None)
    return result


def raw_covers(short, long, short_ref, long_ref, policy=Policy()):
    """Check the final retained witness using a bijection of actual call IDs."""
    if _scope(short, short_ref["dataset"], short_ref["date"]) != _scope(
        long, long_ref["dataset"], long_ref["date"]
    ):
        return False
    if not _eligible(short, policy) or not _eligible(long, policy):
        return False
    if encoded(_guard(short, policy)) != encoded(_guard(long, policy)):
        return False
    for field in ASSETS if policy.extra_information == "preserve_all" else {"tools"}:
        if Counter(map(encoded, short.get(field) or [])) - Counter(
            map(encoded, long.get(field) or [])
        ):
            return False

    def messages(node):
        seen = False
        result = []
        for original in node["messages"]:
            item = _message(original, policy)
            if item["role"] == "assistant" and (item["content"] or item.get("tool_calls")):
                seen = True
            if (
                "empty_assistant" in policy.rules
                and seen
                and item == {"role": "assistant", "content": ""}
                and set(original)
                <= {
                    "role",
                    "content",
                    "tool_calls",
                    "reasoning_content",
                    "reasoning_details",
                    "reasoning",
                }
                and not any(
                    original.get(k) for k in ("reasoning_content", "reasoning_details", "reasoning")
                )
            ):
                continue
            result.append(item)
        return result

    short_messages, long_messages = messages(short), messages(long)
    if not short_messages or len(short_messages) > len(long_messages):
        return False
    bindings = {}
    for a, b in zip(short_messages, long_messages):
        if a["role"] != b["role"]:
            return False
        if "tool_id_alpha" in policy.rules:
            if a["role"] == "assistant":
                ac, bc = a.get("tool_calls") or [], b.get("tool_calls") or []
                if len(ac) != len(bc):
                    return False
                for x, y in zip(ac, bc):
                    bindings[x["id"]] = y["id"]
                    if encoded({k: v for k, v in x.items() if k != "id"}) != encoded(
                        {k: v for k, v in y.items() if k != "id"}
                    ):
                        return False
                a.pop("tool_calls", None)
                b.pop("tool_calls", None)
            elif a["role"] == "tool":
                if bindings.get(a.pop("tool_call_id")) != b.pop("tool_call_id"):
                    return False
        if encoded(a) != encoded(b):
            return False
    return True
