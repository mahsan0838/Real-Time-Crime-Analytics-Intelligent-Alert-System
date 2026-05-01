from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from storm.bolts.multilang_base import ack, emit, fail, log, read_storm_tuple, is_tick_tuple


REQUIRED = {"case_number", "date", "block", "primary_type", "district", "arrest", "latitude", "longitude"}


def main() -> None:
    log("ParseBolt started")
    while True:
        t = read_storm_tuple()
        if t is None:
            return
        if is_tick_tuple(t):
            # nothing to do for ticks
            continue

        if not t.values:
            fail(t.id)
            continue

        # KafkaSpout default translator typically emits:
        # [topic, partition, offset, key, value]
        # but we also support emitting only the value.
        raw = t.values[-1]
        try:
            if isinstance(raw, (bytes, bytearray)):
                raw = raw.decode("utf-8", errors="replace")
            msg = json.loads(raw) if isinstance(raw, str) else raw
            if not isinstance(msg, dict):
                raise ValueError("message is not an object")

            missing = [k for k in REQUIRED if msg.get(k) in (None, "", [])]
            if missing:
                log(f"Malformed message missing {missing}: {msg}", level="warn")
                ack(t.id)
                continue

            emit([json.dumps(msg, ensure_ascii=False)], anchors=[t.id])
            ack(t.id)
        except Exception as e:
            log(f"Parse error: {e} raw={raw!r}", level="error")
            fail(t.id)


if __name__ == "__main__":
    main()

