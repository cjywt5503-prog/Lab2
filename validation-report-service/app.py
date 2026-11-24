import json
import logging
import os
import socket
import sqlite3
import threading
from contextlib import asynccontextmanager
from datetime import timezone

import pika
import uvicorn
from fastapi import FastAPI, HTTPException

from common.consul import ServiceRegistration
from common.contracts import AssayCompletedEvent
from common.storage import database


logger = logging.getLogger(__name__)
INSTANCE_ID = socket.gethostname()
SERVICE_NAME = os.getenv("SERVICE_NAME", "validation-report-service")
SERVICE_PORT = int(os.getenv("SERVICE_PORT", 8003))
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
DB_PATH = os.getenv("REPORT_DB_PATH", "/data/reports.sqlite3")
QUEUE = "assay_completed_queue"
consumer_ready = threading.Event()

# Виключно демонстраційні інтервали для навчального прикладу.
REFERENCE_RANGES = {
    "GLU": {"min": 3.9, "max": 6.1, "unit": "mmol/L"},
    "ALT": {"min": 7.0, "max": 40.0, "unit": "U/L"},
    "CREA": {"min": 62.0, "max": 106.0, "unit": "umol/L"},
}


def init_db():
    with database(DB_PATH) as db:
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("""CREATE TABLE IF NOT EXISTS reports (
            event_id TEXT PRIMARY KEY, barcode TEXT NOT NULL,
            occurred_at TEXT NOT NULL, payload TEXT NOT NULL
        )""")
        db.execute("CREATE INDEX IF NOT EXISTS reports_barcode_time ON reports(barcode, occurred_at)")


def process_event(event: AssayCompletedEvent):
    """ACL: перетворити подію аналізатора на звіт; повтор event_id ідемпотентний."""
    validated_items = []
    for item in event.measurements:
        ref = REFERENCE_RANGES[item.biomarker]
        if item.unit != ref["unit"]:
            raise ValueError(f"Неочікувана одиниця вимірювання для {item.biomarker}")
        validated_items.append({
            "biomarker": item.biomarker,
            "concentration": f"{item.value} {item.unit}",
            "reference_range": f"{ref['min']} - {ref['max']} {ref['unit']}",
            "clinical_status": (
                "NORMAL" if ref["min"] <= item.value <= ref["max"] else "OUTSIDE_REFERENCE_RANGE"
            ),
        })
    occurred_at = event.occurred_at.astimezone(timezone.utc).isoformat(timespec="microseconds")
    report = {
        "report_id": f"REP-{event.event_id}", "event_id": event.event_id,
        "order_id": event.order_id, "barcode": event.barcode, "occurred_at": occurred_at,
        "simulation": True,
        "delta_check": "NOT_IMPLEMENTED", "digital_signature": "NOT_SIGNED",
        "qr_verification": None,
        "validated_results": validated_items,
        "full_processing_trace": (
            f"{event.trace_chain} -> [3. ValidationReportService (instance: {INSTANCE_ID}): "
            "прочитано з RabbitMQ, перевірено демонстраційні референтні інтервали; "
            "DeltaCheck і КЕП не реалізовано]"
        ),
        "consumed_by_instance": INSTANCE_ID,
    }
    with database(DB_PATH) as db:
        db.execute("INSERT OR IGNORE INTO reports VALUES (?, ?, ?, ?)", (
            event.event_id, event.barcode, occurred_at, json.dumps(report, ensure_ascii=False),
        ))


def on_message(channel, method, properties, body):
    try:
        event = AssayCompletedEvent.model_validate_json(body)
        process_event(event)
    except ValueError:
        # Некоректна подія не повинна безкінечно повертатися в робочу чергу.
        logger.exception("Rejected invalid AssayCompletedEvent")
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    except sqlite3.Error:
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        raise  # Перепідключення з паузою, замість циклу без затримки.
    else:
        # Підтвердження лише після успішного COMMIT у сховищі.
        channel.basic_ack(delivery_tag=method.delivery_tag)


def consume_rabbitmq_messages(stopped):
    while not stopped.is_set():
        connection = None
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(
                host=RABBITMQ_HOST, socket_timeout=5, stack_timeout=10,
                blocked_connection_timeout=5, heartbeat=30,
            ))
            channel = connection.channel()
            channel.queue_declare(queue=QUEUE, durable=True)
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue=QUEUE, on_message_callback=on_message, auto_ack=False)
            consumer_ready.set()
            while not stopped.is_set():
                connection.process_data_events(time_limit=1)
        except (pika.exceptions.AMQPError, OSError, sqlite3.Error):
            logger.exception("RabbitMQ consumer will reconnect")
        finally:
            consumer_ready.clear()
            if connection is not None and connection.is_open:
                try:
                    connection.close()
                except pika.exceptions.AMQPError:
                    logger.warning("RabbitMQ connection was already lost")
        stopped.wait(3)


@asynccontextmanager
async def lifespan(app):
    init_db()
    stopped = threading.Event()
    registration = ServiceRegistration(SERVICE_NAME, SERVICE_PORT, INSTANCE_ID)
    worker = threading.Thread(target=consume_rabbitmq_messages, args=(stopped,), daemon=True)
    registration.start()
    worker.start()
    try:
        yield
    finally:
        stopped.set()
        worker.join(timeout=12)
        registration.stop()


app = FastAPI(title="Validation and Report Service (Report Context)", lifespan=lifespan)


@app.get("/health")
def health():
    with database(DB_PATH) as db:
        db.execute("SELECT 1 FROM reports LIMIT 1")
    if not consumer_ready.is_set():
        raise HTTPException(503, "RabbitMQ consumer не підключений")
    return {"status": "UP", "instance": INSTANCE_ID}


@app.get("/reports")
def get_all_reports():
    with database(DB_PATH) as db:
        rows = db.execute("SELECT payload FROM reports ORDER BY occurred_at, event_id").fetchall()
    reports = {}
    for row in rows:
        report = json.loads(row["payload"])
        reports[report["barcode"]] = report
    return {
        "service": SERVICE_NAME, "handled_by_instance": INSTANCE_ID,
        "reports_count": len(reports), "reports": reports,
    }


@app.get("/reports/{barcode}")
def get_report_by_barcode(barcode: str):
    with database(DB_PATH) as db:
        row = db.execute(
            "SELECT payload FROM reports WHERE barcode = ? ORDER BY occurred_at DESC, event_id DESC LIMIT 1",
            (barcode,),
        ).fetchone()
    # Цей сервіс не знає, чи створене замовлення: звіту поки немає.
    return json.loads(row["payload"]) if row else {"status": "REPORT_NOT_AVAILABLE", "barcode": barcode}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=SERVICE_PORT)
