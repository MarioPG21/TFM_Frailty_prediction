#!/usr/bin/env python3
"""
Publica eventos de marcha de la Fuente C (JSONL) al topic Kafka gait-events.

La posición actual se persiste en .gait_state.json junto al script para que
--ticks 1 avance de donde se quedó la última vez.

Uso:
    python spark-apps/scripts/publish_gait.py --ticks 1
    python spark-apps/scripts/publish_gait.py --ticks all --delay 0
"""
import argparse
import json
import os
import sys
import time
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

_SCRIPT_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT = _SCRIPT_DIR.parent.parent
_SYNTHETIC = Path(os.getenv("SYNTHETIC_DATA_PATH",
                             str(_PROJECT_ROOT / "synthetic_data")))
if not _SYNTHETIC.exists():
    _SYNTHETIC = Path("/opt/synthetic_data")

_STATE_FILE = _SCRIPT_DIR / ".gait_state.json"


def _load_state() -> dict:
    if _STATE_FILE.exists():
        return json.loads(_STATE_FILE.read_text())
    return {"next_tick": 0}


def _save_state(state: dict) -> None:
    _STATE_FILE.write_text(json.dumps(state))


def main():
    parser = argparse.ArgumentParser(description="Publica Fuente C a Kafka")
    parser.add_argument("--ticks", default="1", help="Número de meses o 'all'")
    parser.add_argument("--delay", type=float, default=0, help="Segundos entre ticks")
    args = parser.parse_args()

    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:9094")
    topic     = os.getenv("KAFKA_TOPIC_GAIT", "gait-events")

    source_dir = _SYNTHETIC / "source_c" / "gait_events"
    all_files  = sorted(source_dir.glob("*.jsonl"))

    if not all_files:
        print(f"ERROR: No hay ficheros JSONL en {source_dir}", file=sys.stderr)
        sys.exit(1)

    state   = _load_state()
    current = state["next_tick"]

    n_ticks = len(all_files) - current if args.ticks == "all" else int(args.ticks)

    if n_ticks <= 0 or current >= len(all_files):
        print("Nada nuevo que publicar (todos los meses ya fueron enviados).")
        return

    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
    except NoBrokersAvailable:
        print(f"ERROR: No se puede conectar a Kafka en {bootstrap}", file=sys.stderr)
        sys.exit(1)

    published = 0
    for i in range(n_ticks):
        idx = current + i
        if idx >= len(all_files):
            break
        fpath = all_files[idx]
        count = 0
        with open(fpath, encoding="utf-8") as f:
            for line in f:
                producer.send(topic, json.loads(line))
                count += 1
        producer.flush()
        print(f"[{fpath.stem}] gait → {topic}  ({count:,} eventos)")
        published += 1
        if args.delay > 0 and i < n_ticks - 1:
            print(f"Esperando {args.delay:.0f} segundos antes del siguiente tick...")
            time.sleep(args.delay)

    producer.close()
    state["next_tick"] = current + published
    _save_state(state)


if __name__ == "__main__":
    main()
