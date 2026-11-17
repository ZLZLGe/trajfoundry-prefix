"""Snapshot a read-only mask evaluation and submit to the configured queue."""

import argparse
import hashlib
import json
import os
import shutil
import subprocess
from pathlib import Path

import submit_cluster as submit


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--config", type=Path, required=True)
    p.add_argument("--input-plan", type=Path, required=True)
    p.add_argument("--source-manifest-ref", type=Path, required=True)
    p.add_argument("--control", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--partitions", type=int, default=512)
    p.add_argument("--executors", type=int, default=16)
    p.add_argument("--executor-cores", type=int, default=8)
    p.add_argument("--execute", action="store_true")
    a = p.parse_args()
    config = json.loads(a.config.read_text())
    control, output, inputs = [submit.shared_path(x, config["shared_root"])
                               for x in (a.control, a.output, a.input_plan)]
    if control.exists() or output.exists():
        p.error("Use fresh control and output directories")
    control.mkdir(parents=True, mode=0o700)
    (control / "eventLog").mkdir(mode=0o700)
    for directory in ("src", "scripts"):
        shutil.copytree(submit.REPO / directory, control / "app" / directory,
                        ignore=shutil.ignore_patterns("__pycache__"))
    manifest_ref = control / "source-manifest-ref.json"
    manifest_ref.write_bytes(a.source_manifest_ref.read_bytes())
    pod, command = submit.command(config, control, output, input_plan=inputs,
                                  executors=a.executors, executor_cores=a.executor_cores,
                                  partitions=a.partitions, input_cache=False)
    index = command.index("local://" + str(control / "app/scripts/run.py"))
    command[index:] = ["local://" + str(control / "app/scripts/evaluate_masking.py"),
                       "--input-plan", str(inputs), "--output", str(output),
                       "--source-manifest-ref", str(manifest_ref),
                       "--partitions", str(a.partitions),
                       "--s3-endpoint", config["s3_endpoint"],
                       "--s3-region", config["s3_region"],
                       "--s3-credentials-file", config["s3_credentials_file"]]
    for role in ("driver", "executor"):
        template = submit.pod_template(config, control, output, driver=role == "driver", pod=pod)
        (control / (role + "-pod.json")).write_text(json.dumps(template, indent=2) + "\n")
    (control / "empty.properties").write_text("# No inherited Spark defaults.\n")
    source_hashes = {str(f.relative_to(control / "app")): hashlib.sha256(f.read_bytes()).hexdigest()
                     for f in sorted((control / "app").rglob("*")) if f.is_file()}
    (control / "source-hashes.json").write_text(json.dumps(source_hashes, indent=2) + "\n")
    plan = {"driver_pod": pod, "queue": config["queue"], "command": command,
            "output": str(output), "execute": a.execute,
            "repository_commit": subprocess.check_output(
                ["git", "rev-parse", "HEAD"], cwd=submit.REPO, text=True).strip()}
    (control / "plan.json").write_text(json.dumps(plan, indent=2) + "\n")
    print(json.dumps({"event": "prepared", "driver_pod": pod, "queue": config["queue"],
                      "execute": a.execute}), flush=True)
    if not a.execute:
        return
    env = dict(os.environ, JAVA_HOME=config["submit_java_home"])
    for key in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy",
                "PYSPARK_PYTHON", "PYSPARK_DRIVER_PYTHON"):
        env.pop(key, None)
    with (control / "submit.log").open("w") as log:
        result = subprocess.run(command, env=env, stdout=log, stderr=subprocess.STDOUT, check=False)
    state = submit.fetch_pod(config, pod)
    phase = state.get("status", {}).get("phase")
    queue = state["metadata"].get("labels", {}).get("queue")
    report = json.loads((output / "report.json").read_text()) if (output / "report.json").exists() else {}
    passed = (result.returncode == 0 and phase == "Succeeded" and queue == config["queue"]
              and (output / "SUCCEEDED").is_file() and report.get("passed") is True)
    final = {"passed": passed, "driver_pod": pod, "queue": queue,
             "submit_exit_code": result.returncode, "driver_phase": phase}
    (control / "final.json").write_text(json.dumps(final, indent=2) + "\n")
    print(json.dumps(final), flush=True)
    raise SystemExit(0 if passed else 2)


if __name__ == "__main__":
    main()
