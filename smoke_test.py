"""Run against the live Docker Compose stack. Only Python's standard library is required."""
import argparse
import json
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from uuid import uuid4


def request(url, payload=None):
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    req = Request(url, data=data, headers={"Content-Type": "application/json"})
    try:
        with urlopen(req, timeout=20) as response:
            return response.status, json.load(response)
    except HTTPError as exc:
        return exc.code, json.loads(exc.read())


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gateway", default="http://localhost:8080")
    parser.add_argument("--consul", default="http://localhost:8500")
    parser.add_argument("--replicas", type=int, default=3)
    args = parser.parse_args()
    require(args.replicas > 0, "--replicas must be positive")
    api = args.gateway.rstrip("/") + "/api"

    deadline = time.monotonic() + 90
    while True:
        try:
            status, nodes = request(args.consul.rstrip("/") + "/v1/health/service/order-sample-service?passing=true")
            gateway_status, _ = request(api + "/orders")
            if status == 200 and len(nodes) == args.replicas and gateway_status == 200:
                break
        except (URLError, OSError, ValueError):
            pass
        require(time.monotonic() < deadline, "Stack not ready, or replica count differs from --replicas")
        time.sleep(1)

    barcode = "LAB2-" + uuid4().hex[:12]
    order_body = {"patient_id": "PAT-TEST", "barcode": barcode, "biomarkers": ["GLU"]}
    status, created = request(api + "/orders", order_body)
    require(status == 201, f"Create failed: {status} {created}")
    require(request(api + "/orders", order_body)[0] == 409, "Duplicate barcode was not rejected")
    require(request(api + "/orders", {**order_body, "biomarkers": []})[0] == 422, "Empty tests were accepted")

    seen = []
    for _ in range(args.replicas * 2):
        status, result = request(api + "/assays/run", {"barcode": barcode})
        require(status == 200, f"Assay failed: {status} {result}")
        require([item["biomarker"] for item in result["measurements"]] == ["GLU"], "Unordered tests executed")
        selected = result["selected_order_service"]
        require(selected in result["consul_discovered_order_instances"], "Selected address is absent from Consul")
        seen.append(result["handled_by_order_instance"])
        print(f"Order replica: {seen[-1]} | Consul address: {selected}")
    require(len(set(seen)) == args.replicas, "Requests did not reach all order replicas")
    require(seen[:args.replicas] == seen[args.replicas:], "Round Robin cycle differs; use one Assay replica and no concurrent tests")

    deadline = time.monotonic() + 30
    while True:
        status, report = request(api + "/reports/" + barcode)
        require(status == 200, f"Report API failed: {status}")
        if report.get("event_id") == result["event_id"]:
            break
        require(time.monotonic() < deadline, "Report was not received from RabbitMQ in 30 seconds")
        time.sleep(0.5)
    require(report["order_id"] == created["order"]["order_id"], "Report refers to another order")
    require([item["biomarker"] for item in report["validated_results"]] == ["GLU"], "Report has unexpected results")
    require(report["digital_signature"] == "NOT_SIGNED", "Demo falsely claims a signature")
    print(json.dumps(report, ensure_ascii=False, indent=2))

    missing = "MISSING-" + uuid4().hex
    require(request(api + "/orders/by-barcode/" + missing)[0] == 404, "Missing order did not return 404")
    require(request(api + "/assays/run", {"barcode": missing})[0] == 404, "Assay ignored a missing order")
    print("PASS: shared orders, Consul Round Robin, GLU-only assay, RabbitMQ report, 404/409/422")


if __name__ == "__main__":
    main()
