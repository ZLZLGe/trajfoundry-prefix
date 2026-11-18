import json
from dataclasses import asdict
from pathlib import Path

import pytest

from trajectory_prefix.masking import MAX_SPAN, TEMPLATES, VERSION
from trajectory_prefix.projection import Policy
from trajectory_prefix.publication import encoded, sha256
from trajectory_prefix.publication_evidence import part_files_sha256
from trajectory_prefix.publication_job import read_decisions, verify_inspected_decisions
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
    with pytest.raises(ValueError, match="audit" if change == "other_run" else "recheck"):
        read_decisions(spec, source)


@pytest.mark.parametrize("disk", [False, True])
def test_explicitly_skipped_recheck_publishes_after_full_audit(prefix_run, disk):
    root, spec, source, _ = prefix_run
    report_path = root / "report.json"
    report = json.loads(report_path.read_bytes())
    report_path.write_bytes(encoded(dict(report, check_only=True)))
    spec["recheck_report"] = None
    audit_path = Path(spec["audit_report"])
    audit = json.loads(audit_path.read_bytes())
    audit["audit_mode"] = "input_decisions"
    audit["counts"] = {"all": {"input": 3, "kept": 2, "removed": 1,
                               "retained_input_hash_verified": 2, "witness_verified": 1}}
    audit_path.write_bytes(encoded(audit))
    decisions, _ = read_decisions(spec, source, root / "index.sqlite" if disk else None)
    assert len(decisions) == 3
    if disk:
        decisions.db.close()
    for metric in ("input", "kept", "removed", "retained_input_hash_verified", "witness_verified"):
        incomplete = dict(audit["counts"]["all"])
        del incomplete[metric]
        audit_path.write_bytes(encoded(dict(audit, counts={"all": incomplete})))
        with pytest.raises(ValueError, match="audit"):
            read_decisions(spec, source)
    with pytest.raises(KeyError, match="recheck_report"):
        read_decisions({k: v for k, v in spec.items() if k != "recheck_report"}, source)


@pytest.mark.parametrize("empty", [False, True])
@pytest.mark.parametrize("disk", [False, True])
def test_batch_publication_without_debug_jobs(prefix_run, empty, disk):
    root, spec, source, _ = prefix_run
    spec.update(audit_report=None, recheck_report=None)
    if empty:
        source["files"] = []
        spec["decision_files_sha256"] = write_parts(root / "decisions", [])
        Path(spec["input_plan"]).write_bytes(b"")
        spec["input_plan_sha256"] = sha256(b"")
        report = json.loads((root / "report.json").read_bytes())
        report.update(input_count=0, kept_count=0, removed_count=0, input_plan_sha256=spec["input_plan_sha256"])
        (root / "report.json").write_bytes(encoded(report))
    decisions, report = read_decisions(spec, source, root / "index.sqlite" if disk else None)
    inspected = {
        path: {**row["record"], "status": row["status"],
               "final_path": row.get("retained", row["record"])["path"]}
        for path, row in decisions.items()
    }
    assert verify_inspected_decisions(inspected, decisions) == report["removed_count"] == (0 if empty else 1)
    if not empty:
        removed = next(path for path, row in inspected.items() if row["status"] == "removed")
        for change in ({"status": "kept"}, {"final_path": removed}, {"sha256": "d" * 64}):
            with pytest.raises(ValueError, match="Inspected row"):
                verify_inspected_decisions({**inspected, removed: {**inspected[removed], **change}}, decisions)
        del inspected[removed]
        with pytest.raises(ValueError, match="cover all decisions"):
            verify_inspected_decisions(inspected, decisions)
    if disk:
        decisions.db.close()


@pytest.mark.parametrize("change, message", [
    ("input_plan", "input plan"), ("input_digest", "input plan"), ("decisions", "Frozen decisions"),
    ("wrong_root", "final retained input"), ("wrong_id", "frozen manifest"), ("missing_audit_key", "audit_report"),
])
def test_batch_publication_binds_input_and_final_representative(prefix_run, change, message):
    root, spec, source, refs = prefix_run
    spec.update(audit_report=None, recheck_report=None)
    error = ValueError
    if change == "input_plan":
        Path(spec["input_plan"]).write_bytes(b"")
    elif change == "input_digest":
        spec["input_plan_sha256"] = sha256(b"other")
    elif change == "decisions":
        spec["decision_files_sha256"] = sha256(b"other")
    elif change == "missing_audit_key":
        del spec["audit_report"]
        error = KeyError
    else:
        rows = [json.loads(line) for line in (root / "decisions/part-00000").read_text().splitlines()]
        if change == "wrong_root":
            rows[0]["retained"] = refs[0]
        else:
            rows[0]["record"]["id"] = refs[2]["id"]
        spec["decision_files_sha256"] = write_parts(root / "decisions", rows)
    with pytest.raises(error, match=message):
        read_decisions(spec, source)
