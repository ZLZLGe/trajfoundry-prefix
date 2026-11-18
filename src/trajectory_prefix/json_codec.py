"""Decode normalized JSON without rounding structured integer arguments."""

import json
import math
import re

import orjson

# A hit inside a quoted string only selects the slower decoder.
_LONG_INTEGER = re.compile(rb"(?:^|[\[,:])\s*-?[0-9]{19,}\s*(?=[,\]}]|$)")


def _finite_float(token):
    number = float(token)
    if not math.isfinite(number):
        raise ValueError("Non-finite JSON number")
    return number


def loads(data: bytes):
    if _LONG_INTEGER.search(data):
        return json.loads(data.decode("utf-8"), parse_float=_finite_float, parse_constant=_finite_float)
    return orjson.loads(data)


def canonical(value) -> bytes:
    try:
        return orjson.dumps(value, option=orjson.OPT_SORT_KEYS)
    except TypeError as error:
        if str(error) != "Integer exceeds 64-bit range":
            raise
        return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"),
                          allow_nan=False).encode("utf-8")
