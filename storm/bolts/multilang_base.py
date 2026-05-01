from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass
from typing import Any, Iterable


END = "end"


@dataclass(frozen=True)
class StormTuple:
    id: str
    comp: str
    stream: str
    task: int
    values: list[Any]


def _read_message() -> dict[str, Any] | None:
    """
    Storm multilang protocol: each JSON object is terminated by a line containing 'end'.
    Returns None on EOF.
    """
    lines: list[str] = []
    while True:
        line = sys.stdin.readline()
        if line == "":
            return None
        line = line.rstrip("\n")
        if line == END:
            break
        lines.append(line)
    raw = "\n".join(lines).strip()
    if not raw:
        return {}
    return json.loads(raw)


def _write(obj: Any) -> None:
    sys.stdout.write(json.dumps(obj, ensure_ascii=False) + "\n")
    sys.stdout.write(END + "\n")
    sys.stdout.flush()


def log(msg: str, level: str = "info") -> None:
    _write({"command": "log", "msg": msg, "level": level})


def emit(values: Iterable[Any], anchors: list[str] | None = None, stream: str | None = None) -> None:
    payload: dict[str, Any] = {
        "command": "emit",
        "tuple": list(values),
    }
    if anchors is not None:
        payload["anchors"] = anchors
    if stream:
        payload["stream"] = stream
    _write(payload)


def ack(tup_id: str) -> None:
    _write({"command": "ack", "id": tup_id})


def fail(tup_id: str) -> None:
    _write({"command": "fail", "id": tup_id})


def read_storm_tuple() -> StormTuple | None:
    msg = _read_message()
    if msg is None:
        return None
    if not msg:
        return None
    return StormTuple(
        id=str(msg.get("id", "")),
        comp=str(msg.get("comp", "")),
        stream=str(msg.get("stream", "")),
        task=int(msg.get("task", 0)),
        values=list(msg.get("tuple") or []),
    )


def is_tick_tuple(t: StormTuple) -> bool:
    # Storm uses "__tick" component id for tick tuples
    return t.comp == "__tick"


def env_or_default(name: str, default: str) -> str:
    v = os.environ.get(name, "").strip()
    return v if v else default

