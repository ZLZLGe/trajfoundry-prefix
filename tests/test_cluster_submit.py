import importlib.util
import json
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location(
    "prefix_cluster_submit", Path(__file__).parents[1] / "scripts/submit_cluster.py"
)
submit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(submit)


def config(tmp_path):
    template = tmp_path / "base.json"
    template.write_text(
        json.dumps(
            {
                "kind": "Pod",
                "spec": {
                    "volumes": [{"name": "shared", "persistentVolumeClaim": {"claimName": "test"}}],
                    "containers": [
                        {
                            "name": "spark",
                            "volumeMounts": [{"name": "shared", "mountPath": "/share"}],
                        }
                    ],
                },
            }
        )
    )
    return {
        "pod_template": str(template),
        "container_name": "spark",
        "uid": 1001,
        "gid": 100,
        "queue": "user.test",
        "arm_python": "/share/python/bin/python",
        "s3_credentials_file": "/share/private/credentials.json",
        "master": "k8s://https://cluster.test:443",
        "namespace": "test",
        "ca_cert": "/private/ca.crt",
        "oauth_token_file": "/private/token",
        "service_account": "spark",
        "image": "example/spark:3.5",
        "spark_submit": "/opt/spark/bin/spark-submit",
        "s3_endpoint": "https://s3.test",
        "s3_region": "test",
    }


def test_cluster_queue_identity_and_scratch_are_explicit(tmp_path):
    c = config(tmp_path)
    control, output = Path("/share/control"), Path("/share/output")
    pod, cmd = submit.command(c, control, output, input_plan=Path("/share/input.jsonl"))
    assert cmd[cmd.index("--deploy-mode") + 1] == "cluster"
    assert "local" != cmd[cmd.index("--master") + 1]
    conf = dict(cmd[i + 1].split("=", 1) for i, v in enumerate(cmd) if v == "--conf")
    for role in ("driver", "executor"):
        assert conf[f"spark.kubernetes.{role}.label.queue"] == "user.test"
        template = submit.pod_template(c, control, output, driver=role == "driver", pod=pod)
        assert template["spec"]["securityContext"]["runAsUser"] == 1001
        assert template["spec"]["nodeSelector"]["kubernetes.io/arch"] == "arm64"
        assert template["metadata"]["labels"]["queue"] == "user.test"
        assert template["spec"]["containers"][0]["workingDir"] == submit.LOCAL_DIR
    assert conf["spark.kubernetes.trust.certificates"] == "false"
    assert conf["spark.kubernetes.authenticate.submission.oauthTokenFile"] == "/private/token"
    assert conf["spark.pyspark.python"] == c["arm_python"]
    assert not any("s3a://" in item for item in cmd)


def test_cluster_rejects_unmounted_output_and_non_cluster_master(tmp_path):
    c = config(tmp_path)
    with pytest.raises(ValueError, match="mount"):
        submit.pod_template(c, Path("/share/control"), Path("/home/out"), driver=True, pod="probe")
    c["master"] = "local[4]"
    with pytest.raises(ValueError, match="HTTPS"):
        submit.command(c, Path("/share/control"), Path("/share/out"))
    with pytest.raises(ValueError, match="shared"):
        submit.shared_path("/share/../tmp/out", "/share")
