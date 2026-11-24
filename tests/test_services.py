"""Component tests: real FastAPI + SQLite; Consul and AMQP transport are mocked."""
import importlib.util
import json
import sqlite3
import sys
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch
from uuid import uuid4

import pika
import requests
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from common.consul import RoundRobinDiscovery, ServiceRegistration
from common.contracts import AssayCompletedEvent
from common.storage import database


def load_service(folder):
    name = "service_" + uuid4().hex
    spec = importlib.util.spec_from_file_location(name, ROOT / folder / "app.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def response(data, status=200):
    result = Mock(status_code=status)
    result.json.return_value = data
    if status >= 400:
        result.raise_for_status.side_effect = requests.HTTPError(str(status))
    return result


def registry_nodes():
    return [{"Service": {"Address": f"replica-{i}", "Port": 8001}} for i in (3, 1, 2)]


class ServicesTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.orders = [load_service("order-sample-service") for _ in range(3)]
        for i, service in enumerate(self.orders, start=1):
            service.DB_PATH = str(Path(self.temp.name) / "orders.sqlite3")
            service.INSTANCE_ID = f"order-{i}"
            service.init_db()
        self.order_clients = [TestClient(service.app) for service in self.orders]
        self.assay = load_service("assay-execution-service")
        self.assay_client = TestClient(self.assay.app)
        self.reporters = [load_service("validation-report-service") for _ in range(2)]
        for i, service in enumerate(self.reporters, start=1):
            service.DB_PATH = str(Path(self.temp.name) / "reports.sqlite3")
            service.INSTANCE_ID = f"report-{i}"
            service.init_db()
        self.report_clients = [TestClient(service.app) for service in self.reporters]
        for client in [*self.order_clients, self.assay_client, *self.report_clients]:
            self.addCleanup(client.close)

    def create_order(self, barcode="NEW-123", biomarkers=None):
        return self.order_clients[0].post("/orders", json={
            "patient_id": "PAT-NEW", "barcode": barcode,
            "biomarkers": biomarkers if biomarkers is not None else ["GLU"],
        })

    def route_http(self, url, params=None, **kwargs):
        if "/v1/health/service/" in url:
            self.assertEqual(params, {"passing": "true"})
            return response(registry_nodes())
        for i, client in enumerate(self.order_clients, start=1):
            prefix = f"http://replica-{i}:8001"
            if url.startswith(prefix):
                return client.get(url[len(prefix):], params=params)
        self.fail(f"Request bypassed Consul: {url}")

    def event(self, **changes):
        data = dict(
            event_id=str(uuid4()), occurred_at=datetime.now(timezone.utc),
            barcode="NEW-123", order_id="ORD-test", order_instance="order-1",
            assay_instance="assay-1", measurements=[self.assay.DEMO_MEASUREMENTS["GLU"]],
            trace_chain="initial -> OrderService -> AssayService",
        )
        data.update(changes)
        return AssayCompletedEvent(**data)

    def test_create_visible_on_all_replicas_and_after_restart(self):
        created = self.create_order()
        self.assertEqual(created.status_code, 201)
        for client in self.order_clients:
            found = client.get("/orders/by-barcode/NEW-123")
            self.assertEqual(found.json()["order"], created.json()["order"])
        restarted = load_service("order-sample-service")
        restarted.DB_PATH = self.orders[0].DB_PATH
        restarted.init_db()
        self.assertEqual(restarted.get_order_by_barcode("NEW-123")["order"], created.json()["order"])

    def test_unknown_barcode_returns_404_without_fabricating_order(self):
        before = self.order_clients[0].get("/orders").json()["orders_count"]
        self.assertEqual(self.order_clients[1].get("/orders/by-barcode/missing").status_code, 404)
        self.assertEqual(self.order_clients[0].get("/orders").json()["orders_count"], before)

    def test_duplicate_concurrent_creates_are_atomic(self):
        def create(i):
            return self.order_clients[i % 3].post("/orders", json={
                "patient_id": f"PAT-{i}", "barcode": "DUP", "biomarkers": ["GLU"],
            }).status_code
        with ThreadPoolExecutor(max_workers=6) as pool:
            statuses = list(pool.map(create, range(12)))
        self.assertEqual(statuses.count(201), 1)
        self.assertEqual(statuses.count(409), 11)

    def test_invalid_orders_return_422(self):
        cases = [
            {"biomarkers": []}, {"biomarkers": ["UNKNOWN"]},
            {"biomarkers": ["GLU", "GLU"]}, {"patient_id": " "}, {"barcode": "a/b"},
        ]
        for change in cases:
            with self.subTest(change=change):
                payload = {"patient_id": "P", "barcode": "B", "biomarkers": ["GLU"], **change}
                self.assertEqual(self.order_clients[0].post("/orders", json=payload).status_code, 422)

    def test_flow_uses_consul_round_robin_and_only_ordered_tests(self):
        order = self.create_order().json()["order"]
        events = []
        with patch.object(self.assay.requests, "get", side_effect=self.route_http), patch.object(
            self.assay, "publish_assay_completed_event", side_effect=events.append
        ):
            results = [self.assay_client.post("/assays/run", json={"barcode": "NEW-123"}) for _ in range(6)]
        self.assertEqual([result.status_code for result in results], [200] * 6)
        self.assertEqual(
            [result.json()["handled_by_order_instance"] for result in results],
            ["order-1", "order-2", "order-3"] * 2,
        )
        for result, event in zip(results, events):
            self.assertEqual([item["biomarker"] for item in result.json()["measurements"]], ["GLU"])
            self.assertEqual(event.order_id, order["order_id"])
        channel = Mock()
        self.reporters[0].on_message(channel, SimpleNamespace(delivery_tag=7), None, events[-1].model_dump_json())
        channel.basic_ack.assert_called_once_with(delivery_tag=7)
        report = self.report_clients[1].get("/reports/NEW-123").json()
        self.assertEqual(report["event_id"], events[-1].event_id)
        self.assertEqual(report["order_id"], order["order_id"])
        self.assertEqual([r["biomarker"] for r in report["validated_results"]], ["GLU"])
        self.assertIn("OrderService", report["full_processing_trace"])
        self.assertIn("AssayService", report["full_processing_trace"])
        self.assertIn("ValidationReportService", report["full_processing_trace"])
        self.assertEqual(report["digital_signature"], "NOT_SIGNED")
        self.assertEqual(report["delta_check"], "NOT_IMPLEMENTED")

    def test_discovery_errors_return_503(self):
        scenarios = [response([]), response({}, 500), requests.ConnectionError("offline"), response({"bad": 1})]
        for scenario in scenarios:
            with self.subTest(scenario=scenario), patch.object(self.assay.requests, "get") as get, patch.object(
                self.assay, "publish_assay_completed_event"
            ) as publish:
                if isinstance(scenario, Exception):
                    get.side_effect = scenario
                else:
                    get.return_value = scenario
                self.assertEqual(self.assay_client.post("/assays/run", json={}).status_code, 503)
                publish.assert_not_called()

    def test_missing_order_is_404_and_does_not_publish_event(self):
        with patch.object(self.assay.requests, "get", side_effect=self.route_http), patch.object(
            self.assay, "publish_assay_completed_event"
        ) as publish:
            result = self.assay_client.post("/assays/run", json={"barcode": "MISSING"})
        self.assertEqual(result.status_code, 404)
        publish.assert_not_called()

    def test_failed_replica_falls_back_to_another_consul_node(self):
        self.create_order()
        def route(url, **kwargs):
            if url.startswith("http://replica-1:"):
                raise requests.ConnectionError("replica stopped")
            return self.route_http(url, **kwargs)
        with patch.object(self.assay.requests, "get", side_effect=route), patch.object(
            self.assay, "publish_assay_completed_event"
        ):
            result = self.assay_client.post("/assays/run", json={"barcode": "NEW-123"})
        self.assertEqual(result.status_code, 200)
        self.assertEqual(result.json()["selected_order_service"], "replica-2:8001")

    def test_upstream_failure_and_invalid_payload_are_handled(self):
        for upstream, expected in [(response({}, 500), 503), (response({"detail": "bad"}), 502), (response({}, 403), 502)]:
            with self.subTest(expected=expected), patch.object(
                self.assay.discovery, "candidates", return_value=["node:8001"]
            ), patch.object(self.assay.requests, "get", return_value=upstream), patch.object(
                self.assay, "publish_assay_completed_event"
            ) as publish:
                self.assertEqual(self.assay_client.post("/assays/run", json={}).status_code, expected)
                publish.assert_not_called()

    def test_broker_failure_does_not_report_success(self):
        with patch.object(self.assay.requests, "get", side_effect=self.route_http), patch.object(
            self.assay, "publish_assay_completed_event", side_effect=pika.exceptions.AMQPConnectionError()
        ):
            result = self.assay_client.post("/assays/run", json={})
        self.assertEqual(result.status_code, 503)

    def test_ack_happens_after_commit_and_redelivery_is_idempotent(self):
        event = self.event()
        channel = Mock()
        def ack(**kwargs):
            self.assertEqual(self.report_clients[1].get("/reports/NEW-123").json()["event_id"], event.event_id)
        channel.basic_ack.side_effect = ack
        for reporter in self.reporters:
            reporter.on_message(channel, SimpleNamespace(delivery_tag=1), None, event.model_dump_json())
        self.assertEqual(channel.basic_ack.call_count, 2)
        with database(self.reporters[0].DB_PATH) as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM reports").fetchone()[0], 1)
        self.reporters[1].init_db()
        self.assertEqual(self.report_clients[1].get("/reports/NEW-123").json()["consumed_by_instance"], "report-1")

    def test_out_of_order_events_do_not_replace_newest_report(self):
        newer = self.event()
        older = self.event(occurred_at=newer.occurred_at - timedelta(minutes=1))
        self.reporters[0].process_event(newer)
        self.reporters[1].process_event(older)
        self.assertEqual(self.report_clients[0].get("/reports/NEW-123").json()["event_id"], newer.event_id)
        self.assertEqual(self.report_clients[1].get("/reports").json()["reports"]["NEW-123"]["event_id"], newer.event_id)

    def test_invalid_messages_are_rejected_without_requeue(self):
        bad_unit = self.event().model_dump(mode="json")
        bad_unit["measurements"][0]["unit"] = "wrong"
        for body in [b"not-json", b'{}', json.dumps(bad_unit)]:
            channel = Mock()
            with self.assertLogs(level="ERROR"):
                self.reporters[0].on_message(channel, SimpleNamespace(delivery_tag=3), None, body)
            channel.basic_nack.assert_called_once_with(delivery_tag=3, requeue=False)
            channel.basic_ack.assert_not_called()
        self.assertEqual(self.report_clients[0].get("/reports").json()["reports_count"], 0)

    def test_storage_failure_requeues_without_ack(self):
        channel = Mock()
        with patch.object(self.reporters[0], "process_event", side_effect=sqlite3.OperationalError("locked")):
            with self.assertRaises(sqlite3.OperationalError):
                self.reporters[0].on_message(channel, SimpleNamespace(delivery_tag=4), None, self.event().model_dump_json())
        channel.basic_nack.assert_called_once_with(delivery_tag=4, requeue=True)
        channel.basic_ack.assert_not_called()

    def test_abnormal_demo_value_is_not_called_critical(self):
        measurement = {**self.assay.DEMO_MEASUREMENTS["GLU"], "value": 6.2}
        self.reporters[0].process_event(self.event(measurements=[measurement]))
        result = self.report_clients[0].get("/reports/NEW-123").json()
        self.assertEqual(result["validated_results"][0]["clinical_status"], "OUTSIDE_REFERENCE_RANGE")

    def test_health_endpoints(self):
        self.assertEqual(self.order_clients[0].get("/health").status_code, 200)
        self.assertEqual(self.assay_client.get("/health").status_code, 200)
        self.assertEqual(self.report_clients[0].get("/health").status_code, 503)
        self.reporters[0].consumer_ready.set()
        self.assertEqual(self.report_clients[0].get("/health").status_code, 200)

    def test_consul_registration_contains_real_http_health_check(self):
        with patch("common.consul.socket.gethostbyname", return_value="172.19.0.3"):
            registration = ServiceRegistration("order-sample-service", 8001, "replica-1")
        self.assertEqual(registration.payload["Check"]["HTTP"], "http://172.19.0.3:8001/health")
        result = response({}, 500)
        with patch("common.consul.requests.put", return_value=result), patch.object(
            registration.stopped, "wait", side_effect=lambda delay: registration.stopped.set()
        ), self.assertLogs(level="WARNING"):
            registration._register()
        result.raise_for_status.assert_called_once()


if __name__ == "__main__":
    unittest.main()
