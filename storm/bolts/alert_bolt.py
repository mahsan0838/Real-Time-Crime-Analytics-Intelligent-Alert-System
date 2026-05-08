from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import psycopg2
from pymongo import MongoClient

from config.runtime import get_mongo, get_postgres, load_config
from storm.bolts.multilang_base import ack, fail, log, read_storm_tuple, is_tick_tuple


def _severity(count: int, threshold: int) -> str:
    # simple severity levels for rubric; can be tuned
    if count >= threshold * 3:
        return "CRITICAL"
    if count >= threshold * 2:
        return "HIGH"
    return "MEDIUM"


def main() -> None:
    cfg = load_config()
    pg = get_postgres(cfg)
    mongo = get_mongo(cfg)

    pg_conn = psycopg2.connect(
        host=pg.host,
        port=pg.port,
        database=pg.database,
        user=pg.user,
        password=pg.password,
    )
    pg_cur = pg_conn.cursor()

    mongo_client = MongoClient(mongo.host, mongo.port)
    mongo_db = mongo_client[mongo.database]
    mongo_coll = mongo_db[mongo.collection_alerts]

    log(f"AlertBolt started writing to Postgres {pg.host}:{pg.port}/{pg.database} and Mongo {mongo.host}:{mongo.port}/{mongo.database}.{mongo.collection_alerts}")

    while True:
        t = read_storm_tuple()
        if t is None:
            return
        if is_tick_tuple(t):
            continue

        if len(t.values) < 4:
            log("AlertBolt received tuple with insufficient values; dropping", level="warn")
            ack(t.id)
            continue

        try:
            district = str(t.values[0]).strip()
            count = int(t.values[1])
            threshold = int(t.values[2])
            ts = int(t.values[3])

            alert = {
                "district": district,
                "timestamp": datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(),
                "event_count": count,
                "threshold": threshold,
                "severity": _severity(count, threshold),
                "source": "storm-topology",
            }

            # PostgreSQL
            pg_cur.execute(
                "INSERT INTO alerts (district, timestamp, event_count, threshold, severity) VALUES (%s, %s, %s, %s, %s)",
                (district, alert["timestamp"], count, threshold, alert["severity"]),
            )
            pg_conn.commit()

            # MongoDB (full JSON payload)
            mongo_coll.insert_one(alert)

            log(f"Persisted alert: {json.dumps(alert, ensure_ascii=False)}", level="info")
            ack(t.id)
        except Exception as e:
            log(f"AlertBolt error: {e}", level="error")
            ack(t.id)


if __name__ == "__main__":
    main()

