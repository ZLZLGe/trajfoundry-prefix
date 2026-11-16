#!/usr/bin/env bash
set -euo pipefail
unset HTTP_PROXY HTTPS_PROXY ALL_PROXY http_proxy https_proxy all_proxy
[[ "${PREFIX_LOG_PATH:-}" == /* && "${PREFIX_PYTHON:-}" == /* ]] || exit 64
[[ "${1:-}" == driver && -n "${SPARK_DRIVER_BIND_ADDRESS:-}" ]] || exit 64
set -o noclobber
exec > "$PREFIX_LOG_PATH" 2>&1
"$PREFIX_PYTHON" -c '
import json, os, platform, sys
from pathlib import Path
import boto3, orjson, trajectory_prefix
origin = Path(trajectory_prefix.__file__).resolve()
origin.relative_to(Path(os.environ["PREFIX_SOURCE_DIR"]).resolve())
assert os.geteuid() == int(os.environ["PREFIX_UID"])
assert platform.machine() in {"aarch64", "arm64"}
assert sys.version_info[:2] == (3, 11)
print(json.dumps({"event": "driver_runtime", "uid": os.geteuid(),
    "architecture": platform.machine(), "python": sys.executable,
    "package": str(origin), "boto3": boto3.__version__, "orjson": orjson.__version__}), flush=True)
'
shift
exec /opt/entrypoint.sh driver --conf "spark.driver.host=$SPARK_DRIVER_BIND_ADDRESS" "$@"
