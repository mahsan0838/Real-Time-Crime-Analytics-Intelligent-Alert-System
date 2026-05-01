from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.runtime import load_config, resolve_data_path


def main() -> int:
    cfg = load_config()
    keys = ["crime", "arrests", "police_stations", "violence", "sex_offenders"]
    missing = []
    print("Dataset check (from config/config.yaml):")
    for k in keys:
        p = resolve_data_path(cfg, k)
        ok = p.exists()
        print(f"- {k}: {'OK' if ok else 'MISSING'} -> {p}")
        if not ok:
            missing.append(k)

    if missing:
        print("\nMissing datasets:", ", ".join(missing))
        return 2

    print("\nAll datasets present.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

