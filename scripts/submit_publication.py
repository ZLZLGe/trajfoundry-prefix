"""Snapshot and submit the separately authorized S3 publication job."""

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

import submit_cluster as submit


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--publication-config", type=Path, required=True)
    parser.add_argument("--validation-source", type=Path, required=True)
    parser.add_argument("--control", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--executors", type=int, default=16)
    parser.add_argument("--executor-cores", type=int, default=8)
    parser.add_argument("--partitions", type=int, default=512)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    sys.path.insert(0, str(submit.REPO / "src"))
    from trajectory_prefix.comparison import prefix_source_report

    spec_bytes = args.publication_config.read_bytes()
    spec = json.loads(spec_bytes)
    prefix_source_report(spec)
    config = json.loads(args.config.read_text())
    control, output = [submit.shared_path(p, config["shared_root"])
                       for p in (args.control, args.output)]
    if (control.exists() or (output.exists() and not args.resume)
            or control == output or control.is_relative_to(output) or output.is_relative_to(control)):
        parser.error("Use a fresh control directory and a fresh output, or explicit --resume")
    validator = args.validation_source.resolve()
    if not (validator / "validation.py").is_file():
        parser.error("--validation-source must contain the pinned TrajFoundry package")
    for key in ("s3_endpoint", "s3_region", "s3_credentials_file"):
        if spec[key] != config[key]:
            parser.error("Publication S3 settings differ from the cluster settings")
    control.mkdir(parents=True, mode=0o700)
    (control / "eventLog").mkdir(mode=0o700)
    for directory in ("src", "scripts"):
        shutil.copytree(submit.REPO / directory, control / "app" / directory,
                        ignore=shutil.ignore_patterns("__pycache__"))
    shutil.copytree(validator, control / "app/src/trajfoundry",
                    ignore=shutil.ignore_patterns("__pycache__"))
    frozen_spec = control / "publication.json"
    frozen_spec.write_bytes(spec_bytes)
    source_hashes = {str(p.relative_to(control / "app")): hashlib.sha256(p.read_bytes()).hexdigest()
                     for p in sorted((control / "app").rglob("*")) if p.is_file()}
    (control / "source-hashes.json").write_text(json.dumps(source_hashes, indent=2) + "\n")
    pod, command = submit.command(config, control, output, input_plan=frozen_spec,
                                  executors=args.executors, executor_cores=args.executor_cores,
                                  partitions=args.partitions, input_cache=False)
    index = command.index("local://" + str(control / "app/scripts/run.py"))
    command[index:] = ["local://" + str(control / "app/scripts/publish.py"),
                       "--publication-config", str(frozen_spec), "--output", str(output),
                       "--partitions", str(args.partitions)]
    for role in ("driver", "executor"):
        template = submit.pod_template(config, control, output, driver=role == "driver", pod=pod)
        (control / (role + "-pod.json")).write_text(json.dumps(template, indent=2) + "\n")
    (control / "empty.properties").write_text("# No inherited Spark defaults.\n")
    plan = {"driver_pod": pod, "queue": config["queue"], "command": command,
            "output": str(output), "execute": args.execute,
            "normalized_uri": spec["normalized_uri"],
            "publish_selection": spec.get("publish_selection", True),
            "selection_base_uri": spec.get("selection_base_uri"),
            "selection_version": spec.get("selection_version"), "validation_source": str(validator),
            "repository_commit": subprocess.check_output(
                ["git", "rev-parse", "HEAD"], cwd=submit.REPO, text=True).strip()}
    (control / "plan.json").write_text(json.dumps(plan, indent=2) + "\n")
    print(json.dumps({"event": "prepared", "driver_pod": pod, "queue": config["queue"],
                      "execute": args.execute}), flush=True)
    if not args.execute:
        return
    env = dict(os.environ, JAVA_HOME=config["submit_java_home"])
    for key in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy",
                "all_proxy", "PYSPARK_PYTHON", "PYSPARK_DRIVER_PYTHON"):
        env.pop(key, None)
    with (control / "submit.log").open("w") as log:
        result = subprocess.run(command, env=env, stdout=log, stderr=subprocess.STDOUT, check=False)
    state = submit.fetch_pod(config, pod)
    phase = state.get("status", {}).get("phase")
    queue = state["metadata"].get("labels", {}).get("queue")
    deadline = time.monotonic() + 60
    while phase == "Succeeded" and not (output / "SUCCEEDED").is_file() and time.monotonic() < deadline:
        time.sleep(1)
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
