from __future__ import annotations

import json
import sys
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from config.runtime import get_storm, load_config
from storm.bolts.multilang_base import ack, emit, fail, log, read_storm_tuple, is_tick_tuple


@dataclass
class Event:
    ts: float


def main() -> None:
    cfg = load_config()
    s = get_storm(cfg)
    window_seconds = int(s.window_seconds)

    # per-district deque of timestamps
    events: dict[str, deque[Event]] = defaultdict(deque)

    log(f"WindowBolt started window_seconds={window_seconds}")

    while True:
        t = read_storm_tuple()
        if t is None:
            return

        # Tick tuple: emit counts for all districts using current sliding window
        if is_tick_tuple(t):
            now = time.time()
            for district, dq in list(events.items()):
                while dq and (now - dq[0].ts) > window_seconds:
                    dq.popleft()
                emit([district, len(dq), int(now)], anchors=None)
            continue

        if len(t.values) < 2:
            fail(t.id)
            continue

        try:
            district = str(t.values[0]).strip()
            _ = json.loads(t.values[1])  # validate it's JSON; payload not used here

            now = time.time()
            dq = events[district]
            dq.append(Event(ts=now))
            while dq and (now - dq[0].ts) > window_seconds:
                dq.popleft()

            # do not emit per-event; only on ticks (reduces load)
            ack(t.id)
        except Exception as e:
            log(f"WindowBolt error: {e}", level="error")
            fail(t.id)


if __name__ == "__main__":
    main()

