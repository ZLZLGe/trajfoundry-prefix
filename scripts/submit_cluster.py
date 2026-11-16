"""Submit a source snapshot to a configured Kubernetes queue; outputs stay on shared disk."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import shutil
import ssl
import subprocess
import time
from pathlib import Path
from urllib.request import HTTPSHandler, ProxyHandler, Request, build_opener

REPO = Path(__file__).resolve().parents[1]
LOCAL_DIR = "/tmp/trajectory-prefix-spark"


def shared_path(value, shared_root):
    path = Path(value).resolve()
    if not path.is_relative_to(Path(shared_root).resolve()) or path == Path(shared_root).resolve():
        raise ValueError("Path must be inside the configured shared filesystem")
    return path


def pod_template(config, control, output, *, driver, pod):
    value = json.loads(Path(config["pod_template"]).read_text())
    if value.get("kind") != "Pod":
        raise ValueError("Expected a JSON Pod template")
    value = copy.deepcopy(value)
    spec = value.setdefault("spec", {})
    selected = [c for c in spec["containers"] if c["name"] == config["container_name"]]
    if len(selected) != 1:
        raise ValueError("Ambiguous application container")
    container = selected[0]
    shared_volumes = {
        v["name"]
        for v in spec.get("volumes", [])
        if any(k in v for k in ("persistentVolumeClaim", "nfs", "csi"))
    }
    mounts = [
        Path(m["mountPath"])
        for m in container.get("volumeMounts", [])
        if m["name"] in shared_volumes
    ]
    for required in (
        control,
        output,
        Path(config["arm_python"]),
        Path(config["s3_credentials_file"]),
    ):
        if not any(required == m or required.is_relative_to(m) for m in mounts):
            raise ValueError("Pod template does not mount a required shared path")
    spec.setdefault("nodeSelector", {})["kubernetes.io/arch"] = "arm64"
    identity = {"runAsUser": config["uid"], "runAsGroup": config["gid"], "runAsNonRoot": True}
    spec.setdefault("securityContext", {}).update(identity)
    container.setdefault("securityContext", {}).update(identity)
    spec.pop("activeDeadlineSeconds", None)
    name = "spark-local-dir-prefix"
    if any(v["name"].startswith("spark-local-dir-") for v in spec.get("volumes", [])):
        raise ValueError("Base template already has a Spark scratch volume")
    spec.setdefault("volumes", []).append({"name": name, "emptyDir": {}})
    container.setdefault("volumeMounts", []).append({"name": name, "mountPath": LOCAL_DIR})
    # Spark fetches addPyFile archives into the executor working directory.
    # The image's default /opt/spark/work-dir is not writable by the user's UID.
    container["workingDir"] = LOCAL_DIR
    env = {
        "PYTHONPATH": str(control / "app/src"),
        "PYSPARK_PYTHON": config["arm_python"],
        "PYTHONUNBUFFERED": "1",
        "TMPDIR": LOCAL_DIR,
        "JAVA_HOME": config.get("pod_java_home", "/opt/java/openjdk"),
    }
    if driver:
        container["command"] = [
            "/bin/bash",
            str(control / "app/scripts/cluster_driver_entrypoint.sh"),
        ]
        container.pop("args", None)
        env.update(
            PREFIX_LOG_PATH=str(control / "driver.log"),
            PREFIX_PYTHON=config["arm_python"],
            PREFIX_SOURCE_DIR=str(control / "app/src"),
            PREFIX_UID=str(config["uid"]),
        )
    container["env"] = [e for e in container.get("env", []) if e["name"] not in env]
    container["env"] += [{"name": k, "value": v} for k, v in env.items()]
    value.setdefault("metadata", {}).setdefault("labels", {}).update(
        {"queue": config["queue"], "trajectory-prefix-run": pod}
    )
    return value


def command(
    config,
    control,
    output,
    *,
    input_plan=None,
    probe=False,
    audit_run=None,
    partitions=512,
    executors=16,
    executor_cores=8,
    rules=None,
):
    if not config["master"].startswith("k8s://https://"):
        raise ValueError("Cluster submission requires Kubernetes HTTPS")
    if not config["queue"]:
        raise ValueError("An explicit queue is required")
    pod = "trajectory-prefix-" + hashlib.sha256(str(control).encode()).hexdigest()[:16]
    conf = {
        "spark.kubernetes.driver.master": config["master"].removeprefix("k8s://"),
        "spark.kubernetes.namespace": config["namespace"],
        "spark.kubernetes.authenticate.submission.caCertFile": config["ca_cert"],
        "spark.kubernetes.authenticate.submission.oauthTokenFile": config["oauth_token_file"],
        "spark.kubernetes.trust.certificates": "false",
        "spark.kubernetes.driver.pod.name": pod,
        "spark.kubernetes.authenticate.driver.serviceAccountName": config["service_account"],
        "spark.kubernetes.authenticate.executor.serviceAccountName": config["service_account"],
        "spark.pyspark.python": config["arm_python"],
        "spark.pyspark.driver.python": config["arm_python"],
        "spark.kubernetes.driverEnv.PYTHONPATH": str(control / "app/src"),
        "spark.executorEnv.PYTHONPATH": str(control / "app/src"),
        "spark.executorEnv.PYTHONUNBUFFERED": "1",
        "spark.driver.cores": "2" if probe else "4",
        "spark.driver.memory": "4g" if probe else "8g",
        "spark.driver.memoryOverhead": "4g" if probe else "8g",
        "spark.driver.maxResultSize": "2g",
        "spark.executor.instances": str(executors),
        "spark.executor.cores": str(executor_cores),
        "spark.kubernetes.executor.request.cores": str(executor_cores),
        "spark.kubernetes.executor.limit.cores": str(executor_cores),
        "spark.executor.memory": "4g" if probe else "16g",
        "spark.executor.memoryOverhead": "4g" if probe else "48g",
        "spark.dynamicAllocation.enabled": "false",
        "spark.speculation": "false",
        "spark.task.maxFailures": "4",
        "spark.stage.maxConsecutiveAttempts": "4",
        "spark.excludeOnFailure.enabled": "true",
        "spark.network.timeout": "600s",
        "spark.local.dir": LOCAL_DIR,
        "spark.jars.ivy": LOCAL_DIR + "/ivy",
        "spark.kubernetes.local.dirs.tmpfs": "false",
        "spark.kubernetes.submission.waitAppCompletion": "true",
        "spark.kubernetes.executor.deleteOnTermination": "true",
        "spark.kubernetes.driver.service.deleteOnTermination": "true",
        "spark.eventLog.enabled": "true",
        "spark.eventLog.dir": str(control / "eventLog"),
        "spark.ui.showConsoleProgress": "false",
    }
    for role in ("driver", "executor"):
        conf.update(
            {
                f"spark.kubernetes.{role}.container.image": config["image"],
                f"spark.kubernetes.{role}.podTemplateFile": str(control / f"{role}-pod.json"),
                f"spark.kubernetes.{role}.podTemplateContainerName": config["container_name"],
                f"spark.kubernetes.{role}.label.queue": config["queue"],
                f"spark.kubernetes.{role}.label.trajectory-prefix-run": pod,
            }
        )
    if config.get("extra_listener"):
        conf["spark.extraListeners"] = config["extra_listener"]
    result = [
        config["spark_submit"],
        "--properties-file",
        str(control / "empty.properties"),
        "--master",
        config["master"],
        "--deploy-mode",
        "cluster",
        "--name",
        pod,
    ]
    for k, v in conf.items():
        result += ["--conf", f"{k}={v}"]
    application = "cluster_probe.py" if probe else "audit.py" if audit_run else "run.py"
    result += [
        "local://" + str(control / "app/scripts" / application),
        "--output",
        str(output),
    ]
    if not probe:
        result += [
            "--source-run" if audit_run else "--input-plan",
            str(audit_run or input_plan),
            "--partitions",
            str(partitions),
            "--s3-endpoint",
            config["s3_endpoint"],
            "--s3-region",
            config["s3_region"],
            "--s3-credentials-file",
            config["s3_credentials_file"],
            "--cache-dir",
            LOCAL_DIR + "/input-cache",
        ]
        if rules is not None and not audit_run:
            result += ["--rules", rules]
    return pod, result


def fetch_pod(config, pod):
    context = ssl.create_default_context(cafile=config["ca_cert"])
    context.verify_flags |= ssl.VERIFY_X509_PARTIAL_CHAIN
    opener = build_opener(ProxyHandler({}), HTTPSHandler(context=context))
    request = Request(
        config["master"].removeprefix("k8s://").rstrip("/")
        + "/api/v1/namespaces/"
        + config["namespace"]
        + "/pods/"
        + pod,
        headers={"Authorization": "Bearer " + Path(config["oauth_token_file"]).read_text().strip()},
    )
    with opener.open(request, timeout=30) as response:
        return json.load(response)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--config", type=Path, required=True)
    p.add_argument("--control", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--input-plan")
    p.add_argument("--probe", action="store_true")
    p.add_argument("--audit-run")
    p.add_argument("--executors", type=int, default=16)
    p.add_argument("--executor-cores", type=int, default=8)
    p.add_argument("--partitions", type=int, default=512)
    p.add_argument("--rules")
    p.add_argument("--execute", action="store_true")
    a = p.parse_args()
    c = json.loads(a.config.read_text())
    control, output = [shared_path(x, c["shared_root"]) for x in (a.control, a.output)]
    if control == output or control.is_relative_to(output) or output.is_relative_to(control):
        p.error("Control and output must be separate non-nested directories")
    if control.exists() or output.exists():
        p.error("Use fresh control and output directories")
    if min(a.executors, a.executor_cores, a.partitions) < 1:
        p.error("Resources and partitions must be positive")
    if sum(bool(x) for x in (a.probe, a.input_plan, a.audit_run)) != 1:
        p.error("Choose exactly one of --input-plan, --probe, --audit-run")
    audit_run = shared_path(a.audit_run, c["shared_root"]) if a.audit_run else None
    if audit_run and not (audit_run / "SUCCEEDED").is_file():
        p.error("Audit source run has not completed")
    input_plan = shared_path(a.input_plan, c["shared_root"]) if a.input_plan else None
    if input_plan and not input_plan.is_file():
        p.error("Input plan does not exist")
    pod, cmd = command(
        c,
        control,
        output,
        input_plan=input_plan,
        probe=a.probe,
        audit_run=audit_run,
        executors=a.executors,
        executor_cores=a.executor_cores,
        partitions=a.partitions,
        rules=a.rules,
    )
    templates = {
        r: pod_template(c, control, output, driver=r == "driver", pod=pod)
        for r in ("driver", "executor")
    }
    control.mkdir(parents=True, mode=0o700)
    (control / "eventLog").mkdir(mode=0o700)
    for directory in ("src", "scripts"):
        shutil.copytree(
            REPO / directory,
            control / "app" / directory,
            ignore=shutil.ignore_patterns("__pycache__"),
        )
    files = {
        str(f.relative_to(control / "app")): hashlib.sha256(f.read_bytes()).hexdigest()
        for f in sorted((control / "app").rglob("*"))
        if f.is_file()
    }
    (control / "source-hashes.json").write_text(json.dumps(files, indent=2) + "\n")
    for r, template in templates.items():
        (control / f"{r}-pod.json").write_text(json.dumps(template, indent=2) + "\n")
    (control / "empty.properties").write_text("# No inherited Spark defaults.\n")
    plan = {
        "driver_pod": pod,
        "queue": c["queue"],
        "command": cmd,
        "probe": a.probe,
        "source_access": "read-only",
        "output": str(output),
        "execute": a.execute,
    }
    (control / "plan.json").write_text(json.dumps(plan, indent=2) + "\n")
    print(
        json.dumps(
            {
                "event": "prepared",
                "control": str(control),
                "driver_pod": pod,
                "queue": c["queue"],
                "execute": a.execute,
            }
        ),
        flush=True,
    )
    if not a.execute:
        return
    env = dict(os.environ, JAVA_HOME=c["submit_java_home"])
    for key in (
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
        "PYSPARK_PYTHON",
        "PYSPARK_DRIVER_PYTHON",
    ):
        env.pop(key, None)
    with (control / "submit.log").open("w") as log:
        result = subprocess.run(cmd, env=env, stdout=log, stderr=subprocess.STDOUT, check=False)
    pod_state = fetch_pod(c, pod)
    phase = pod_state.get("status", {}).get("phase")
    labels = pod_state["metadata"].get("labels", {})
    # A succeeded Pod can be visible before another NFS client invalidates its
    # negative directory cache. Allow publication to become visible locally.
    deadline = time.monotonic() + 60
    while (
        phase == "Succeeded"
        and not (output / "SUCCEEDED").is_file()
        and time.monotonic() < deadline
    ):
        time.sleep(1)
    report = (
        json.loads((output / "report.json").read_text())
        if (output / "report.json").exists()
        else {}
    )
    evidence = (output / "SUCCEEDED").is_file() and report.get("spark_master", "").startswith(
        "k8s://"
    )
    passed = (
        result.returncode == 0
        and phase == "Succeeded"
        and evidence
        and labels.get("queue") == c["queue"]
    )
    final = {
        "passed": passed,
        "driver_pod": pod,
        "queue": labels.get("queue"),
        "submit_exit_code": result.returncode,
        "driver_phase": phase,
        "output_verified": evidence,
    }
    (control / "final.json").write_text(json.dumps(final, indent=2) + "\n")
    print(json.dumps(final), flush=True)
    raise SystemExit(0 if passed else 2)


if __name__ == "__main__":
    main()
