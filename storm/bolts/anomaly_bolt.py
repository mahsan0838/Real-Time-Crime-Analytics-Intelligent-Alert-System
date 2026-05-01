from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from config.runtime import get_storm, load_config
from storm.bolts.multilang_base import ack, emit, fail, log, read_storm_tuple, is_tick_tuple


def main() -> None:
    cfg = load_config()
    s = get_storm(cfg)
    threshold = int(s.anomaly_threshold)
    log(f"AnomalyBolt started threshold={threshold}")

    while True:
        t = read_storm_tuple()
        if t is None:
            return
        if is_tick_tuple(t):
            continue

        if len(t.values) < 3:
            fail(t.id)
            continue

        try:
            district = str(t.values[0]).strip()
            count = int(t.values[1])
            ts = int(t.values[2])

            if count > threshold:
                emit([district, count, threshold, ts], anchors=[t.id])
            ack(t.id)
        except Exception as e:
            log(f"AnomalyBolt error: {e}", level="error")
            fail(t.id)


if __name__ == "__main__":
    main()

