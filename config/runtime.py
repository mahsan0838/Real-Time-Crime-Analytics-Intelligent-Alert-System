from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
import os


def project_root() -> Path:
    # config/runtime.py -> config -> project root
    return Path(__file__).resolve().parents[1]


def load_config() -> dict[str, Any]:
    cfg_path = project_root() / "config" / "config.yaml"
    if not cfg_path.exists():
        return {}
    return yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}


def _running_in_docker() -> bool:
    # Common signals for Linux containers
    if os.environ.get("RUNNING_IN_DOCKER", "").lower() in {"1", "true", "yes"}:
        return True
    return Path("/.dockerenv").exists()


def resolve_data_path(cfg: dict[str, Any], key: str) -> Path:
    """
    Resolve a dataset path from config.yaml.

    key: one of {crime, arrests, police_stations, violence, sex_offenders}
    """
    root = project_root()
    rel = (((cfg.get("data_paths") or {}).get(key)) or "").strip()
    if not rel:
        return root / "data" / f"{key}.csv"
    p = (root / rel).resolve()
    return p


@dataclass(frozen=True)
class KafkaSettings:
    broker: str
    topic_in: str
    topic_out: str
    producer_rate: int


@dataclass(frozen=True)
class StormSettings:
    window_seconds: int
    slide_seconds: int
    anomaly_threshold: int


@dataclass(frozen=True)
class PostgresSettings:
    host: str
    port: int
    database: str
    user: str
    password: str


@dataclass(frozen=True)
class MongoSettings:
    host: str
    port: int
    database: str
    collection_alerts: str


def get_kafka(cfg: dict[str, Any]) -> KafkaSettings:
    k = cfg.get("kafka") or {}
    return KafkaSettings(
        broker=str(k.get("broker") or "localhost:9092"),
        topic_in=str(k.get("topic") or "crime_events"),
        topic_out=str(k.get("topic_out") or "crime_alerts"),
        producer_rate=int(k.get("producer_rate") or 1),
    )


def get_storm(cfg: dict[str, Any]) -> StormSettings:
    s = cfg.get("storm") or {}
    window_seconds = int(s.get("window_length_minutes") or 5) * 60
    slide_seconds = int(s.get("slide_interval_minutes") or 1) * 60
    return StormSettings(
        window_seconds=window_seconds,
        slide_seconds=slide_seconds,
        anomaly_threshold=int(s.get("anomaly_threshold") or 100),
    )


def get_postgres(cfg: dict[str, Any]) -> PostgresSettings:
    p = cfg.get("postgres") or {}
    host = str(p.get("host") or "localhost")
    port = int(p.get("port") or 5432)

    # If scripts run inside docker-compose containers, use service DNS names.
    if _running_in_docker() and host in {"localhost", "127.0.0.1", "::1"}:
        host = "postgres"
        port = 5432

    return PostgresSettings(
        host=host,
        port=port,
        database=str(p.get("database") or "crime_analytics"),
        user=str(p.get("user") or "crime_user"),
        password=str(p.get("password") or "crime_pass"),
    )


def get_mongo(cfg: dict[str, Any]) -> MongoSettings:
    m = cfg.get("mongodb") or {}
    host = str(m.get("host") or "localhost")
    port = int(m.get("port") or 27017)

    if _running_in_docker() and host in {"localhost", "127.0.0.1", "::1"}:
        host = "mongodb"
        port = 27017

    return MongoSettings(
        host=host,
        port=port,
        database=str(m.get("database") or "crime_analytics"),
        collection_alerts=str(m.get("collection_alerts") or "alert_logs"),
    )

