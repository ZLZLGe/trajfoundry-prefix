"""Verified immutable reads and atomic local/shared-filesystem output. No S3 writes."""

from __future__ import annotations

import hashlib
import json
import os
import stat
import tempfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from urllib.parse import urlsplit

import orjson


@dataclass(frozen=True)
class StorageConfig:
    endpoint_url: str | None = None
    region: str = "us-east-1"
    credentials_file: str | None = None
    cache_dir: str | None = None


def relative_path(value: str) -> str:
    if not isinstance(value, str) or not value or "\\" in value or any(ord(c) < 32 for c in value):
        raise ValueError("Unsafe relative path")
    p = PurePosixPath(value)
    if p.is_absolute() or any(p in {"", ".", ".."} for p in value.split("/")):
        raise ValueError("Unsafe relative path")
    return str(p)


def reference(entry: dict) -> dict:
    result = dict(entry)
    for field in ("dataset", "date", "path"):
        result[field] = relative_path(result[field])
    if "/" in result["dataset"] or "/" in result["date"]:
        raise ValueError("Dataset and date must be single path components")
    result["id"] = "/".join(result[k] for k in ("dataset", "date", "path"))
    if len(result.get("sha256", "")) != 64 or any(
        c not in "0123456789abcdef" for c in result["sha256"]
    ):
        raise ValueError("Every input needs a SHA256 digest")
    if type(result.get("bytes")) is not int or result["bytes"] < 1:
        raise ValueError("Every input needs its positive byte length")
    return result


def credentials(path: str) -> dict:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        st = os.fstat(descriptor)
        if (
            not stat.S_ISREG(st.st_mode)
            or st.st_uid != os.geteuid()
            or stat.S_IMODE(st.st_mode) not in {0o400, 0o600}
            or st.st_size > 65536
        ):
            raise ValueError("Credential file owner, mode or type is unsafe")
        with os.fdopen(descriptor, "rb", closefd=False) as handle:
            value = json.load(handle)
    finally:
        os.close(descriptor)
    required = {"aws_access_key_id", "aws_secret_access_key"}
    if (
        not isinstance(value, dict)
        or not required <= value.keys()
        or value.keys() - required - {"aws_session_token"}
    ):
        raise ValueError("Credential file schema is invalid")
    return value


class Reader:
    def __init__(self, config: StorageConfig):
        self.config = config
        self.client = None

    def read(self, entry: dict) -> bytes:
        local = entry.get("local_path")
        cached = Path(self.config.cache_dir) / entry["sha256"] if self.config.cache_dir else None
        if local and Path(local).is_file():
            data = Path(local).read_bytes()
        elif cached and cached.is_file():
            data = cached.read_bytes()
        else:
            uri = urlsplit(entry.get("uri", ""))
            if (
                uri.scheme != "s3"
                or not uri.netloc
                or uri.username
                or uri.password
                or uri.query
                or uri.fragment
            ):
                raise ValueError("Input is unavailable locally and has no safe S3 URI")
            if self.client is None:
                import boto3
                from botocore.config import Config

                params = (
                    credentials(self.config.credentials_file)
                    if self.config.credentials_file
                    else {}
                )
                self.client = boto3.client(
                    "s3",
                    endpoint_url=self.config.endpoint_url,
                    region_name=self.config.region,
                    config=Config(retries={"mode": "standard", "max_attempts": 8}),
                    **params,
                )

                def guard(model, **kwargs):
                    if model.name != "GetObject":
                        raise RuntimeError("Only GetObject is permitted")

                self.client.meta.events.register("before-parameter-build.s3.*", guard)
            response = self.client.get_object(Bucket=uri.netloc, Key=uri.path.lstrip("/"))
            try:
                data = response["Body"].read()
            finally:
                response["Body"].close()
        if len(data) != entry["bytes"] or hashlib.sha256(data).hexdigest() != entry["sha256"]:
            raise ValueError("Input length or SHA256 changed: " + entry["id"])
        if cached and not cached.exists():
            cached.parent.mkdir(parents=True, exist_ok=True)
            atomic_bytes(cached, data, entry["sha256"])
        return data

    def node(self, entry: dict) -> dict:
        # Existing normalized files each contain one complete root object.
        # Multi-record physical shards must first be indexed into individual records.
        value = orjson.loads(self.read(entry))
        if not isinstance(value, dict):
            raise ValueError("One complete normalized root object per file is required")
        return value


def atomic_bytes(target: Path, data: bytes, expected_sha: str):
    target.parent.mkdir(parents=True, exist_ok=True)
    descriptor, name = tempfile.mkstemp(prefix=".prefix-", dir=target.parent)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(data)
        try:
            os.link(name, target)
        except FileExistsError:
            if hashlib.sha256(target.read_bytes()).hexdigest() != expected_sha:
                raise FileExistsError("Output collision with different bytes") from None
    finally:
        Path(name).unlink(missing_ok=True)
