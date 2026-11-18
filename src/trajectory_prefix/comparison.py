"""Comparison modes accepted by active jobs and by stored-decision readers."""

import json
from pathlib import Path

PREFIX_RULES = (
    "billing_cch", "assistant_trailing_space", "tool_id_alpha", "empty_assistant",
    "claude_preamble_cch",
)


def validate_rules(rules):
    unknown = set(rules).difference(PREFIX_RULES)
    if unknown:
        raise ValueError("Unknown prefix comparison rule: " + ", ".join(sorted(unknown)))


def require_prefix(metadata):
    if "mask_version" in metadata or metadata.get("comparison_mode", "prefix") != "prefix":
        raise ValueError("Only prefix comparison is supported. Relaxed masking is disabled.")


def prefix_source_report(spec):
    require_prefix(spec)
    report = json.loads((Path(spec["source_run"]) / "report.json").read_bytes())
    require_prefix(report)
    return report


def reusable_prefix_report(directory):
    directory = Path(directory)
    completed = directory / "report.json"
    if completed.is_file():
        require_prefix(json.loads(completed.read_bytes()))
    prior = json.loads((directory / "decision-report.json").read_bytes())
    require_prefix(prior)
    return prior
