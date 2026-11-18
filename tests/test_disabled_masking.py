import json
import subprocess
import sys
from copy import deepcopy
from pathlib import Path

import pytest
from test_tool_ids import sample

from trajectory_prefix import mask_job, refinement_job
from trajectory_prefix.comparison import reusable_prefix_report
from trajectory_prefix.projection import Policy, covers, project
from trajectory_prefix.publication_job import run as publish
from trajectory_prefix.verification import raw_covers

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"


@pytest.mark.parametrize("script", [
    "submit_mask_evaluation.py", "submit_refinement.py", "refine.py",
    "evaluate_masking.py", "explain_masking.py",
])
def test_retired_scripts_stop_before_creating_outputs(tmp_path, script):
    proc = subprocess.run([sys.executable, "-I", "-S", str(SCRIPTS / script), "--execute"],
                          cwd=tmp_path, capture_output=True, text=True)
    assert proc.returncode and "masking is disabled" in proc.stderr
    assert not list(tmp_path.iterdir())


def test_direct_job_entry_points_refuse_masking_before_io(tmp_path):
    with pytest.raises(ValueError, match="masking is disabled"):
        mask_job.run(None, None, None, None, None, None)
    with pytest.raises(ValueError, match="masking is disabled"):
        refinement_job.run(None, None, None, None, None, None, None)
    spec = tmp_path / "spec.json"
    spec.write_text('{"comparison_mode":"placeholder-relaxed-v1"}')
    with pytest.raises(ValueError, match="masking is disabled"):
        publish(spec, tmp_path / "output")
    assert not (tmp_path / "output").exists()


@pytest.mark.parametrize("filename", ["report.json", "decision-report.json"])
def test_legacy_mask_result_cannot_be_reused(tmp_path, filename):
    (tmp_path / "decision-report.json").write_text("{}")
    (tmp_path / filename).write_text(json.dumps({"mask_version": "placeholder-relaxed-v1"}))
    with pytest.raises(ValueError, match="masking is disabled"):
        reusable_prefix_report(tmp_path)


def test_literal_mask_tokens_and_tool_arguments_in_ordinary_coverage():
    policy = Policy(extra_information="messages_only")
    ref = {"dataset": "sample", "date": "2026-10-30"}
    short = sample("x", True)
    short["messages"][0]["content"] = "fill <MASK>"
    short["messages"][1]["tool_calls"][0]["function"]["arguments"] = {"q": "<PERSON>"}
    long = deepcopy(short)
    long["messages"].append({"role": "user", "content": "next"})
    for q, args, match in [("fill <MASK>", "<PERSON>", True), ("fill Paris", "<PERSON>", False),
                           ("fill <MASK>", "somebody", False), ("fill [MASK]", "<PERSON>", False)]:
        long["messages"][0]["content"] = q
        long["messages"][1]["tool_calls"][0]["function"]["arguments"] = {"q": args}
        assert covers(project(short, "sample", ref["date"], policy),
                      project(long, "sample", ref["date"], policy)) is match
        assert raw_covers(short, long, ref, ref, policy) is match
