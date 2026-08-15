# hw3_api.py
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, Sequence, Dict, Any, Optional

import requests

from .util import log


class _NodeLike(Protocol):
    name: str
    external_port: int
    ip: str
    index: int


@dataclass
class KvsClientException(Exception):
    message: str


class KvsTimeout(Exception):
    def __str__(self):
        return "request timed out"


class CreateClient(Protocol):
    def __call__(self, name: str) -> "KvsClient": ...


class KvsFixture:
    def __init__(self):
        self.clients: list[KvsClient] = []

    def create_client(self, name: str) -> "KvsClient":
        client = KvsClient(name=name)
        self.clients.append(client)
        return client


class KvsClient:
    def __init__(self, name: str, timeout: int = 10, num_retries: int = 3):
        self.name = name
        self.timeout = timeout
        self.num_retries = num_retries
        self.causal_metadata = {}
        self._log = []
        self._id = 0

    def _new_id(self) -> int:
        id = self._id
        self._id += 1
        return id

    def dump_logs(self, path: Path) -> None:
        """Dump the logs to a file"""
        path.mkdir(parents=True, exist_ok=True)
        with (path / f"{self.name}.jsonl").open("w") as f:
            for item in self._log:
                json.dump(item, f)
                f.write("\n")

    def _base_url(self, node: _NodeLike) -> str:
        return f"http://localhost:{node.external_port}"

    def _request(self, corr_id: int, node: _NodeLike, method: str, path: str, **kwargs) -> requests.Response:
        # send request, but handle some exceptions
        url = f"{self._base_url(node)}/{path.lstrip('/')}"
        response = None
        timed_out = False

        # Add causal metadata to the request body if it exists
        if "json" in kwargs and self.causal_metadata:
            kwargs["json"]["causal-metadata"] = self.causal_metadata
        elif method != "DELETE":  # DELETE doesn't have a body
            if "json" not in kwargs:
                kwargs["json"] = {}
            kwargs["json"]["causal-metadata"] = self.causal_metadata

        try:
            for i in range(self.num_retries):
                try:
                    response = getattr(requests, method)(url, timeout=self.timeout, **kwargs)
                    if response.status_code == 500:
                        return response
                    break
                except requests.exceptions.ConnectionError:
                    if i == self.num_retries - 1:
                        raise
                    time.sleep(0.5)  # Short delay before retry

            if response is None:
                raise KvsClientException(f"failed to connect after {self.num_retries} attempts")

            # Update causal metadata from response if available
            if response.ok and "causal-metadata" in response.json():
                self.causal_metadata = response.json()["causal-metadata"]

            return response

        except requests.exceptions.Timeout:
            timed_out = True
            res = requests.Response()
            res.status_code = 408
            return res
        finally:
            log_entry = {
                "id": corr_id,
                "url": url,
                "method": method,
                "payload": kwargs.get("json"),
                "status_code": response.status_code if response is not None else None,
                "response_text": response.text if response is not None else None,
                "timed_out": timed_out,
            }
            self._log.append(log_entry)

    def ping(self, node: _NodeLike) -> bool:
        """Test if a node is responsive"""
        id = self._new_id()
        log(f"client {self.name} [{id}] -> {node.name}: ping")
        try:
            res = self._request(id, node, "get", "ping")
            return res.status_code == 200
        except Exception:
            return False

    def put(self, node: _NodeLike, key: str, value: str, causal_metadata: Any) -> Dict[str, Any]:
        """Put a key-value pair into the store and return response data"""
        id = self._new_id()
        log(f"client {self.name} [{id}] -> {node.name}: put {key!r} := {value!r}")
        if len(key) == 0:
            raise ValueError("key cannot be empty")

        res = self._request(id, node, "put", f"data/{key}", json={"value": value, "causal-metadata": causal_metadata})

        response_data = {"status_code": res.status_code, "ok": res.ok}

        if res.ok:
            response_data["causal_metadata"] = res.json().get("causal-metadata", {})

        return response_data

    def get(self, node: _NodeLike, key: str, causal_metadata: Any) -> Dict[str, Any]:
        """Get a value for a key from the store and return response data"""
        id = self._new_id()
        log(f"client {self.name} [{id}] -> {node.name}: get {key!r}")
        if len(key) == 0:
            raise ValueError("key cannot be empty")

        res = self._request(id, node, "get", f"data/{key}", json={"causal-metadata": causal_metadata})

        response_data = {"status_code": res.status_code, "ok": res.ok, "value": None}

        if res.ok:
            data = res.json()
            response_data["value"] = data.get("value")
            response_data["causal_metadata"] = data.get("causal-metadata", {})
            log(f"client {self.name} [{id}] -> {node.name}: get {key!r} |> {response_data['value']!r}")

        return response_data

    def get_all(self, node: _NodeLike, causal_metadata: Any) -> Dict[str, Any]:
        """Get all key-value pairs from the store and return response data"""
        id = self._new_id()
        log(f"client {self.name} [{id}] -> {node.name}: list")

        res = self._request(id, node, "get", "data", json={})

        response_data = {"status_code": res.status_code, "ok": res.ok, "values": {}}

        if res.ok:
            data = res.json()
            response_data["items"] = data.get("items", {})
            response_data["causal-metadata"] = data.get("causal-metadata", {})
            log(f"client {self.name} [{id}] -> {node.name}: list |> [{len(response_data['items'])} items]")

        return response_data

    def send_view(self, node: _NodeLike, view: Sequence[_NodeLike]) -> bool:
        """Send a view update to a node"""
        id = self._new_id()
        view_ = [dict(address=f"{n.ip}:8081", id=n.index) for n in view]
        log(
            f"client {self.name} [{id}] -> {node.name}: view {[f'{n.name} (addr={n.ip}:8081, id={n.index})' for n in view]}"
        )

        res = self._request(id, node, "put", "view", json={"view": view_})
        return res.ok

    def broadcast_view(self, nodes: Sequence[_NodeLike]) -> bool:
        """Broadcast a view update to all nodes"""
        log(f"client {self.name}: broadcast view")
        success = True
        for node in nodes:
            if not self.send_view(node, nodes):
                success = False
        return success

    def reset_causal_metadata(self):
        """Reset the client's causal metadata to empty"""
        self.causal_metadata = {}
