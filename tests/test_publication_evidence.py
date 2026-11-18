import json
from dataclasses import asdict
from pathlib import Path

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


@pytest.mark.parametrize("declared_mode", [VERSION, "prefix", None])
@pytest.mark.parametrize("disk", [False, True])
def test_mask_publication_is_disabled_even_if_relabelled(masking_run, declared_mode, disk):
    root, spec, source, _, _ = masking_run
    if declared_mode is None:
        del spec["comparison_mode"]
    else:
        spec["comparison_mode"] = declared_mode
    with pytest.raises(ValueError, match="masking is disabled"):
        read_decisions(spec, source, root / "index.sqlite" if disk else None)
    assert not (root / "index.sqlite").exists()


@pytest.fixture
def prefix_run(masking_run):
    root, spec, source, refs, _ = masking_run
    spec.pop("comparison_mode")
    report = {"policy": spec["policy"], "input_count": 3, "kept_count": 2, "removed_count": 1,
              "input_plan": spec["input_plan"], "input_plan_sha256": spec["input_plan_sha256"]}
    (root / "report.json").write_bytes(encoded(report))
    audit = {"passed": True, "source_run": str(root), "policy": spec["policy"],
             "input_plan_sha256": spec["input_plan_sha256"]}
    retained_plan = root / "retained-input.jsonl"
    retained_plan.write_bytes(b"".join(encoded(r) for r in refs[1:]))
    check = {"policy": spec["policy"], "input_count": 2, "kept_count": 2, "removed_count": 0,
             "input_plan": str(retained_plan), "input_plan_sha256": sha256(retained_plan.read_bytes())}
    for field, body in (("audit_report", audit), ("recheck_report", check)):
        folder = root / field
        folder.mkdir()
        (folder / "SUCCEEDED").touch()
        path = folder / "report.json"
        path.write_bytes(encoded(body))
        spec[field] = str(path)
    return root, spec, source, refs


@pytest.mark.parametrize("mode", [None, "prefix"])
def test_ordinary_prefix_receipts_remain_compatible(prefix_run, mode):
    _, spec, source, _ = prefix_run
    if mode:
        spec["comparison_mode"] = mode
    decisions, report = read_decisions(spec, source)
    assert len(decisions) == 3 and report["kept_count"] == 2


@pytest.mark.parametrize("change", ["other_run", "different_member", "duplicate_member", "different_bytes",
                                    "different_date", "changed_digest", "missing_success"])
def test_prefix_publication_binds_audit_and_recheck_to_actual_members(prefix_run, change):
    root, spec, source, refs = prefix_run
    if change == "other_run":
        path = Path(spec["audit_report"])
        audit = json.loads(path.read_bytes())
        path.write_bytes(encoded(dict(audit, source_run=str(root.parent))))
    elif change == "missing_success":
        (Path(spec["recheck_report"]).parent / "SUCCEEDED").unlink()
    else:
        path = Path(spec["recheck_report"])
        check = json.loads(path.read_bytes())
        plan = Path(check["input_plan"])
        if change == "changed_digest":
            plan.write_bytes(plan.read_bytes() + b"\n")
        else:
            other = {"different_member": refs[0], "duplicate_member": refs[1],
                     "different_bytes": dict(refs[2], sha256="d" * 64),
                     "different_date": dict(refs[2], date="2026-08-22")}[change]
            plan.write_bytes(encoded(refs[1]) + encoded(other))
            path.write_bytes(encoded(dict(check, input_plan_sha256=sha256(plan.read_bytes()))))
    with pytest.raises(ValueError, match="recheck"):
        read_decisions(spec, source)
