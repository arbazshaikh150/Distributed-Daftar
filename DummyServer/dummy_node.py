#!/usr/bin/env python3

import argparse
import json
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Optional


@dataclass
class DummyNode:
    host: str
    port: int
    total_capacity: int
    available_capacity: int
    node_id: Optional[str] = None


def post_json(url: str, payload: dict) -> dict:
    data = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    with urllib.request.urlopen(request, timeout=10) as response:
        body = response.read().decode("utf-8")
        if not body:
            return {}
        return json.loads(body)


def register_node(server: str, node: DummyNode) -> None:
    payload = {
        "host": node.host,
        "port": node.port,
        "totalCapacity": node.total_capacity,
    }

    response = post_json(f"{server}/nodes/register", payload)
    node.node_id = response["nodeId"]
    print(f"registered node: {node.node_id}")


def send_heartbeat(server: str, node: DummyNode) -> None:
    if node.node_id is None:
        raise ValueError("node must be registered before heartbeat")

    payload = {
        "nodeId": node.node_id,
        "availableCapacity": node.available_capacity,
        "totalCapacity": node.total_capacity,
    }

    response = post_json(f"{server}/nodes/heartbeat", payload)
    print(f"heartbeat sent: {response}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Register a dummy node and send heartbeats.")
    parser.add_argument("--server", default="http://localhost:8080")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=9001)
    parser.add_argument("--total-capacity", type=int, default=1_000_000)
    parser.add_argument("--available-capacity", type=int, default=1_000_000)
    parser.add_argument("--interval", type=int, default=2)
    args = parser.parse_args()

    node = DummyNode(
        host=args.host,
        port=args.port,
        total_capacity=args.total_capacity,
        available_capacity=args.available_capacity,
    )

    try:
        register_node(args.server, node)

        while True:
            send_heartbeat(args.server, node)
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nstopped dummy node")
    except (urllib.error.URLError, urllib.error.HTTPError, KeyError, ValueError) as err:
        print(f"dummy node error: {err}")


if __name__ == "__main__":
    main()
