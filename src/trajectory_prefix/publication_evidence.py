"""Validate frozen masking decisions and their independent audit before publication."""

from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path

from .masking import MAX_SPAN, TEMPLATES, VERSION
from .projection import Policy
from .publication import encoded, sha256
from .storage import file_sha256, reference


def part_files_sha256(directory):
    directory = Path(directory)
    if not (directory / "_SUCCESS").is_file():
        raise ValueError("Incomplete evidence directory: " + str(directory))
    inventory = {p.name: file_sha256(p) for p in sorted(directory.glob("part-*"))}
    if not inventory:
        raise ValueError("Missing evidence parts: " + str(directory))
    return sha256(encoded(inventory))


def frozen_rows(directory, expected_sha256):
    directory = Path(directory)
    if part_files_sha256(directory) != expected_sha256:
        raise ValueError("Frozen evidence files changed: " + str(directory))
    for path in sorted(directory.glob("part-*")):
        with path.open() as stream:
            for line in stream:
                if line.strip():
                    yield json.loads(line)


def validate_mask_report(spec, report):
    run = Path(spec["source_run"])
    path = run / "report.json"
    expected_policy = json.loads(json.dumps(asdict(Policy(extra_information="messages_only"))))
    v = report["verification"]
    if not (
        file_sha256(path) == spec["source_report_sha256"]
        and json.loads(path.read_text()) == report
        and report.get("passed") is True
        and report["policy"] == spec["policy"] == expected_policy
        and report["mask_version"] == spec["comparison_mode"] == VERSION
        and report["max_placeholder_chars"] == MAX_SPAN
        and report["literal_templates"] == sorted(TEMPLATES)
        and report["source_manifest_sha256"] == spec["source_manifest_sha256"]
        and report["input_plan_sha256"] == spec["input_plan_sha256"]
        == file_sha256(Path(spec["input_plan"]))
        and report["input_count"] == report["kept_count"] + report["removed_count"]
        and v["source_hashes"] == report["input_count"]
        and v["removed_witnesses"] == report["removed_count"]
        and v["retained_recheck_hashes"] == report["kept_count"]
        and v["retained_recheck_matches"] == 0
        and v["source_manifest_unchanged"] is True
    ):
        raise ValueError("Completed frozen masking audit and zero-match recheck are required")


def validate_mask_decisions(spec, report, decisions):
    run = Path(spec["source_run"])
    records = {row["record"]["id"]: row["record"] for row in decisions.values()}
    if len(records) != len(decisions):
        raise ValueError("Duplicate decision reference")
    inputs = set()
    with Path(spec["input_plan"]).open() as stream:
        for line in stream:
            if not line.strip():
                continue
            ref = reference(json.loads(line))
            if ref["id"] in inputs or records.get(ref["id"]) != ref:
                raise ValueError("Masking input plan differs from publication decisions")
            inputs.add(ref["id"])
    if inputs != records.keys():
        raise ValueError("Masking input plan is incomplete")

    removed = {row["record"]["id"]: row for row in decisions.values()
               if row["status"] == "removed"}
    audited = set()
    for row in frozen_rows(run / "audit", spec["audit_files_sha256"]):
        key = row["record"]["id"]
        decision = removed.get(key)
        if (key in audited or decision is None or row.get("passed") is not True
                or row["record"] != decision["record"]
                or row["retained"] != decision["retained"]):
            raise ValueError("Independent masking witness differs from final decision")
        audited.add(key)
    if audited != removed.keys():
        raise ValueError("Independent masking audit does not cover every removal")

    kept = {row["record"]["id"]: row["record"] for row in decisions.values()
            if row["status"] == "kept"}
    retained = set()
    for row in frozen_rows(run / "retained-input", spec["retained_files_sha256"]):
        key = row["id"]
        if key in retained or kept.get(key) != row:
            raise ValueError("Retained recheck inputs differ from final decisions")
        retained.add(key)
    if retained != kept.keys():
        raise ValueError("Retained recheck inputs are incomplete")

    checked = set()
    for row in frozen_rows(run / "recheck-checks", spec["recheck_files_sha256"]):
        pair = (row["short"]["id"], row["long"]["id"])
        if (pair in checked or row.get("matched") is not False
                or kept.get(pair[0]) != row["short"] or kept.get(pair[1]) != row["long"]):
            raise ValueError("Retained masking recheck contains an invalid or matching pair")
        checked.add(pair)
    if len(checked) != report["verification"]["retained_recheck_candidates"]:
        raise ValueError("Retained masking recheck coverage differs from report")


def validate_mask_decisions_streamed(spec, report, decisions):
    """Same evidence checks with uniqueness tables, without duplicate RAM indexes."""
    run, db = Path(spec["source_run"]), decisions.db
    db.execute("CREATE TABLE evidence_seen (kind TEXT, key TEXT, PRIMARY KEY(kind,key))")

    def seen(kind, key):
        db.execute("INSERT INTO evidence_seen VALUES(?,?)", (kind, key))

    def verify_ref(ref):
        normalized = reference(ref)
        decision = decisions.get(normalized["path"])
        if decision is None or decision["record"] != normalized:
            raise ValueError("Evidence reference differs from frozen decision")
        return decision

    n = 0
    with Path(spec["input_plan"]).open() as stream:
        for line in stream:
            if line.strip():
                ref = reference(json.loads(line))
                verify_ref(ref)
                seen("input", ref["id"])
                n += 1
    if n != len(decisions):
        raise ValueError("Incomplete masking publication input")
    n = 0
    for row in frozen_rows(run / "audit", spec["audit_files_sha256"]):
        decision = verify_ref(row["record"])
        if (decision["status"] != "removed" or row.get("passed") is not True
                or row["retained"] != decision["retained"]):
            raise ValueError("Independent masking witness differs from final decision")
        seen("audit", row["record"]["id"])
        n += 1
    if n != report["removed_count"]:
        raise ValueError("Incomplete independent masking audit")
    n = 0
    for ref in frozen_rows(run / "retained-input", spec["retained_files_sha256"]):
        if verify_ref(ref)["status"] != "kept":
            raise ValueError("Recheck input is not retained")
        seen("kept", ref["id"])
        n += 1
    if n != report["kept_count"]:
        raise ValueError("Incomplete retained recheck input")
    n = 0
    for row in frozen_rows(run / "recheck-checks", spec["recheck_files_sha256"]):
        if (row.get("matched") is not False or verify_ref(row["short"])["status"] != "kept"
                or verify_ref(row["long"])["status"] != "kept"):
            raise ValueError("Invalid retained masking recheck")
        seen("pair", json.dumps([row["short"]["id"], row["long"]["id"]]))
        n += 1
    if n != report["verification"]["retained_recheck_candidates"]:
        raise ValueError("Incomplete retained masking recheck")
    db.commit()
