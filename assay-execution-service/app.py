import os
import socket
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from uuid import uuid4

import pika
import requests
import uvicorn
from fastapi import FastAPI, HTTPException

from common.consul import RoundRobinDiscovery, ServiceRegistration
from common.contracts import AssayCompletedEvent, AssayRunRequest, OrderLookup


INSTANCE_ID = socket.gethostname()
SERVICE_NAME = os.getenv("SERVICE_NAME", "assay-execution-service")
SERVICE_PORT = int(os.getenv("SERVICE_PORT", 8002))
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
QUEUE = "assay_completed_queue"
discovery = RoundRobinDiscovery()

# Навчальні фіксовані значення, а не результати роботи реального аналізатора.
DEMO_MEASUREMENTS = {
    "GLU": {"biomarker": "GLU", "optical_density": 0.412, "value": 5.4, "unit": "mmol/L"},
    "ALT": {"biomarker": "ALT", "optical_density": 0.195, "value": 28.0, "unit": "U/L"},
    "CREA": {"biomarker": "CREA", "optical_density": 0.310, "value": 82.5, "unit": "umol/L"},
}


@asynccontextmanager
async def lifespan(app):
    registration = ServiceRegistration(SERVICE_NAME, SERVICE_PORT, INSTANCE_ID)
    registration.start()
    try:
        yield
    finally:
        registration.stop()


app = FastAPI(title="Assay Execution Service (Assay Context)", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "UP", "instance": INSTANCE_ID}


def load_order(req: AssayRunRequest, nodes: list[str]):
    # Безпечний повтор GET на наступній репліці, якщо обрана репліка недоступна.
    for node in nodes:
        try:
            response = requests.get(
                f"http://{node}/orders/by-barcode/{req.barcode}",
                params={"client_note": req.initial_message}, timeout=5,
            )
        except requests.RequestException:
            continue
        if response.status_code == 404:
            raise HTTPException(404, "Замовлення з таким штрихкодом не знайдено")
        if response.status_code >= 500:
            continue
        if response.status_code != 200:
            raise HTTPException(502, "OrderService повернув неочікуваний HTTP-статус")
        try:
            result = OrderLookup.model_validate(response.json())
            if result.order.barcode != req.barcode:
                raise ValueError("Barcode mismatch")
        except (ValueError, TypeError) as exc:
            raise HTTPException(502, "OrderService повернув некоректні дані") from exc
        return result, node
    raise HTTPException(503, "Усі знайдені репліки OrderService недоступні")


def publish_assay_completed_event(event: AssayCompletedEvent):
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=RABBITMQ_HOST, socket_timeout=5, stack_timeout=10,
        blocked_connection_timeout=5, heartbeat=30,
    ))
    try:
        channel = connection.channel()
        channel.queue_declare(queue=QUEUE, durable=True)
        channel.confirm_delivery()
        channel.basic_publish(
            exchange="", routing_key=QUEUE, mandatory=True,
            body=event.model_dump_json().encode("utf-8"),
            properties=pika.BasicProperties(
                delivery_mode=2, content_type="application/json", message_id=event.event_id,
            ),
        )
    finally:
        if connection.is_open:
            connection.close()


@app.post("/assays/run")
def run_assay(req: AssayRunRequest):
    nodes = discovery.candidates("order-sample-service")
    order_response, selected_node = load_order(req, nodes)
    measurements = [DEMO_MEASUREMENTS[code].copy() for code in order_response.order.biomarkers]
    trace = (
        f"{order_response.trace_chain} -> [2. AssayService (instance: {INSTANCE_ID}): "
        "згенеровано демонстраційні результати лише призначених аналізів]"
    )
    event = AssayCompletedEvent(
        event_id=str(uuid4()), occurred_at=datetime.now(timezone.utc),
        barcode=req.barcode, order_id=order_response.order.order_id,
        order_instance=order_response.handled_by_order_instance,
        assay_instance=INSTANCE_ID, measurements=measurements, trace_chain=trace,
    )
    try:
        publish_assay_completed_event(event)
    except (pika.exceptions.AMQPError, OSError) as exc:
        raise HTTPException(503, "RabbitMQ не підтвердив публікацію події") from exc
    return {
        "status": "ASSAY_COMPLETED_AND_EVENT_PUBLISHED", "event_id": event.event_id,
        "simulation": True,
        "consul_discovered_order_instances": sorted(nodes),
        "selected_order_service": selected_node,
        "handled_by_order_instance": order_response.handled_by_order_instance,
        "handled_by_assay_instance": INSTANCE_ID,
        "trace_chain": trace, "measurements": measurements,
        "async_broker_queue": f"{QUEUE} (RabbitMQ)",
    }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=SERVICE_PORT)
