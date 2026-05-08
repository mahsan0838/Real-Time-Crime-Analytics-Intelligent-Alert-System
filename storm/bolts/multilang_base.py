from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass
from typing import Any, Iterable


END = "end"


@dataclass(frozen=True)
class StormTuple:
    # Keep original ID type from Storm (often int/long).
    # Converting to str breaks ack/fail correlation in ShellBolt.
    id: Any
    comp: str
    stream: str
    task: int
    values: list[Any]


def _normalize_values(raw_values: Any) -> list[Any]:
    if raw_values is None:
        return []
    if isinstance(raw_values, list):
        return raw_values
    return [raw_values]


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


def ack(tup_id: Any) -> None:
    # FluxShellBolt runs with auto-acking semantics; sending explicit ack
    # from Python can produce duplicate-ack errors and crash the worker.
    return


def fail(tup_id: Any) -> None:
    # Keep runtime stable under malformed tuples by avoiding explicit fail.
    # Errors are still surfaced through log() calls.
    return


def read_storm_tuple() -> StormTuple | None:
    while True:
        msg = _read_message()
        if msg is None:
            # true EOF: subprocess should terminate
            return None

        # Ignore protocol/control frames that are not tuples.
        if not msg:
            continue

        # Storm multilang can deliver tuples either as an object:
        # {"id","comp","stream","task","tuple"}
        # or as a positional array:
        # [id, comp, stream, task, tuple_values]
        if isinstance(msg, list):
            if len(msg) < 5:
                continue
            comp = str(msg[1] or "")
            stream = str(msg[2] or "")
            values = _normalize_values(msg[4])
            # Drop non-tuple control frames emitted by the multilang protocol.
            if not comp and not stream and not values:
                continue
            return StormTuple(
                id=msg[0],
                comp=comp,
                stream=stream,
                task=int(msg[3] or 0),
                values=values,
            )

        if isinstance(msg, dict):
            # init/pid/heartbeat/control messages may not have tuple payload.
            if "tuple" not in msg:
                continue
            values = _normalize_values(msg.get("tuple"))
            comp = str(msg.get("comp", ""))
            stream = str(msg.get("stream", ""))
            if not comp and not stream and not values:
                continue
            return StormTuple(
                id=msg.get("id"),
                comp=comp,
                stream=stream,
                task=int(msg.get("task", 0)),
                values=values,
            )

        # Unknown frame type: ignore and keep reading.
        continue


def is_tick_tuple(t: StormTuple) -> bool:
    # Tick tuples can be represented as (__system, __tick) or with "__tick" component.
    return t.comp == "__tick" or t.stream == "__tick" or (t.comp == "__system" and t.stream == "__tick")


def env_or_default(name: str, default: str) -> str:
    v = os.environ.get(name, "").strip()
    return v if v else default

