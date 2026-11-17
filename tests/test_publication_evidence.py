import json
from dataclasses import asdict

import pytest

from trajectory_prefix.masking import MAX_SPAN, TEMPLATES, VERSION
from trajectory_prefix.projection import Policy
from trajectory_prefix.publication import encoded, sha256
from trajectory_prefix.publication_evidence import part_files_sha256
from trajectory_prefix.publication_job import read_decisions
from trajectory_prefix.storage import reference


def write_parts(path, rows):
    path.mkdir(exist_ok=True)
    (path / "part-00000").write_bytes(b"".join(encoded(row) for row in rows))
    (path / "_SUCCESS").touch()
    return part_files_sha256(path)


@pytest.fixture
def masking_run(tmp_path):
    source_uri = "s3://bucket/sxf/normalized/v004/dt=2026-08-21/"
    refs = [reference({"dataset": "sxf", "date": "2026-08-21", "path": c + ".jsonl",
                       "bytes": 200, "sha256": c * 64, "uri": source_uri + c + ".jsonl"})
            for c in "abc"]
    decisions = [{"record": refs[0], "retained": refs[1], "status": "removed"},
                 {"record": refs[1], "status": "kept"},
                 {"record": refs[2], "status": "kept"}]
    inputs = tmp_path / "inputs.jsonl"
    inputs.write_bytes(b"".join(encoded(ref) for ref in refs))
    policy = json.loads(json.dumps(asdict(Policy(extra_information="messages_only"))))
    report = {"passed": True, "input_count": 3, "kept_count": 2, "removed_count": 1,
              "policy": policy, "mask_version": VERSION, "max_placeholder_chars": MAX_SPAN,
              "literal_templates": sorted(TEMPLATES), "source_manifest_sha256": "m" * 64,
              "input_plan_sha256": sha256(inputs.read_bytes()),
              "verification": {"source_hashes": 3, "removed_witnesses": 1,
                               "retained_recheck_hashes": 2, "retained_recheck_candidates": 1,
                               "retained_recheck_matches": 0, "source_manifest_unchanged": True}}
    (tmp_path / "report.json").write_bytes(encoded(report))
    (tmp_path / "SUCCEEDED").touch()
    spec = {"source_run": str(tmp_path), "source_uri": source_uri, "policy": policy,
            "dataset": "sxf", "date": "2026-08-21", "comparison_mode": VERSION,
            "input_plan": str(inputs), "input_plan_sha256": report["input_plan_sha256"],
            "source_manifest_sha256": report["source_manifest_sha256"],
            "source_report_sha256": sha256(encoded(report)),
            "decision_files_sha256": write_parts(tmp_path / "decisions", decisions),
            "audit_files_sha256": write_parts(tmp_path / "audit", [
                {"record": refs[0], "retained": refs[1], "passed": True}]),
            "retained_files_sha256": write_parts(tmp_path / "retained-input", refs[1:]),
            "recheck_files_sha256": write_parts(tmp_path / "recheck-checks", [
                {"short": refs[1], "long": refs[2], "matched": False}])}
    source = {"files": [{k: ref[k] for k in ("path", "bytes", "sha256")} for ref in refs]}
    return tmp_path, spec, source, refs, report


def test_frozen_masking_publication_accepts_complete_direct_witnesses(masking_run):
    _, spec, source, refs, report = masking_run
    decisions, result = read_decisions(spec, source)
    assert result == report
    assert len(decisions) == 3
    assert decisions["a.jsonl"]["retained"] == refs[1]


@pytest.mark.parametrize("change", ["changed_audit", "missing_audit", "wrong_witness",
                                   "unpassed_audit", "missing_retained", "matched_recheck",
                                   "missing_recheck", "changed_decision", "missing_success",
                                   "changed_input", "changed_source", "changed_policy"])
def test_masking_publication_rejects_invalid_or_changed_evidence(masking_run, change):
    root, spec, source, refs, _ = masking_run
    if change == "changed_audit":
        (root / "audit/part-00000").write_bytes(b"[]\n")
    elif change == "missing_audit":
        spec["audit_files_sha256"] = write_parts(root / "audit", [])
    elif change in {"wrong_witness", "unpassed_audit"}:
        spec["audit_files_sha256"] = write_parts(root / "audit", [{
            "record": refs[0], "retained": refs[2] if change == "wrong_witness" else refs[1],
            "passed": change != "unpassed_audit"}])
    elif change == "missing_retained":
        spec["retained_files_sha256"] = write_parts(root / "retained-input", [refs[1]])
    elif change == "matched_recheck":
        spec["recheck_files_sha256"] = write_parts(root / "recheck-checks", [{
            "short": refs[1], "long": refs[2], "matched": True}])
    elif change == "missing_recheck":
        spec["recheck_files_sha256"] = write_parts(root / "recheck-checks", [])
    elif change == "changed_decision":
        with (root / "decisions/part-00000").open("a") as stream:
            stream.write("\n")
    elif change == "missing_success":
        (root / "audit/_SUCCESS").unlink()
    elif change == "changed_input":
        with (root / "inputs.jsonl").open("a") as stream:
            stream.write("\n")
    elif change == "changed_source":
        spec["source_manifest_sha256"] = "x" * 64
    elif change == "changed_policy":
        spec["policy"] = {**spec["policy"], "extra_information": "tools_only"}
    with pytest.raises(ValueError):
        read_decisions(spec, source)


@pytest.mark.parametrize("key,value", [("mask_version", "unknown"),
                                      ("max_placeholder_chars", MAX_SPAN + 1),
                                      ("literal_templates", []), ("passed", False)])
def test_masking_publication_rejects_different_rule_boundaries(masking_run, key, value):
    root, spec, source, _, report = masking_run
    report[key] = value
    (root / "report.json").write_bytes(encoded(report))
    spec["source_report_sha256"] = sha256(encoded(report))
    with pytest.raises(ValueError, match="frozen masking"):
        read_decisions(spec, source)


def test_existing_prefix_publication_remains_compatible(masking_run):
    root, spec, source, _, report = masking_run
    spec.pop("comparison_mode")
    report["input_plan"] = spec["input_plan"]
    (root / "report.json").write_bytes(encoded(report))
    audit = {"passed": True, "policy": report["policy"],
             "input_plan_sha256": report["input_plan_sha256"]}
    recheck = {"policy": report["policy"], "input_count": 2, "removed_count": 0}
    for name, value in [("audit_report", audit), ("recheck_report", recheck)]:
        path = root / (name + ".json")
        path.write_bytes(encoded(value))
        spec[name] = str(path)
    decisions, _ = read_decisions(spec, source)
    assert len(decisions) == 3
