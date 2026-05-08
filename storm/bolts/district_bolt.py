from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from storm.bolts.multilang_base import ack, emit, fail, log, read_storm_tuple, is_tick_tuple


def main() -> None:
    log("DistrictBolt started")
    while True:
        t = read_storm_tuple()
        if t is None:
            return
        if is_tick_tuple(t):
            continue

        if not t.values:
            log("DistrictBolt received empty tuple values; dropping", level="warn")
            ack(t.id)
            continue

        try:
            msg = json.loads(t.values[0])
            district = str(msg.get("district") or "000").strip()
            emit([district, json.dumps(msg, ensure_ascii=False)], anchors=[t.id])
            ack(t.id)
        except Exception as e:
            log(f"DistrictBolt error: {e}", level="error")
            ack(t.id)


if __name__ == "__main__":
    main()

