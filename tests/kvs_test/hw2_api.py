import json
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, Sequence

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


@dataclass
class PutResponse:
    status_code: int
    existed: bool
    ok: bool


@dataclass
class GetResponse:
    status_code: int
    value: str | None
    ok: bool


@dataclass
class DeleteResponse:
    status_code: int
    ok: bool


@dataclass
class GetAllResponse:
    status_code: int
    values: dict[str, str]
    ok: bool


@dataclass
class _LogItem:
    id: int
    url: str
    method: str
    payload: dict | None
    timed_out: bool
    status_code: int | None
    response_text: str | None

    def json(self) -> dict:
        return {
            "id": self.id,
            "url": self.url,
            "method": self.method,
            "payload": self.payload,
            "timed_out": self.timed_out,
            "status_code": self.status_code,
            "response_text": self.response_text,
        }


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
    def __init__(self, name: str, timeout: int = 10, num_retries: int = 500):
        self.name = name
        self.timeout = timeout
        self.num_retries = num_retries

        self._log: list[_LogItem] = []
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
                json.dump(item.json(), f)
                f.write("\n")

    def _base_url(self, node: _NodeLike) -> str:
        return f"http://localhost:{node.external_port}"

    def _request(self, corr_id: int, node: _NodeLike, method: str, path: str, **kwargs) -> requests.Response:
        # send request, but handle some exceptions
        url = f"{self._base_url(node)}/{path.lstrip('/')}"
        response = None
        timed_out = False
        try:
            for i in range(self.num_retries):
                try:
                    response = getattr(requests, method)(url, timeout=self.timeout, **kwargs)
                    # check if the response is a server error
                    if response.status_code == 500:
                        return response
                    break
                except requests.exceptions.ConnectionError:
                    continue
            if response is None:
                raise KvsClientException(f"failed to connect after {self.num_retries} attempts")

            return response
        except requests.exceptions.Timeout:
            timed_out = True
            res = requests.Response()
            res.status_code = 408
            return res
        finally:
            self._log.append(
                _LogItem(
                    id=corr_id,
                    url=url,
                    method=method,
                    payload=kwargs.get("json"),
                    status_code=response.status_code if response is not None else None,
                    response_text=response.text if response is not None else None,
                    timed_out=timed_out,
                )
            )

    def ping(self, node: _NodeLike):
        id = self._new_id()
        log(f"client {self.name} [{id}] -> {node.name}: ping")
        res = self._request(id, node, "get", "ping")
        if not res.ok:
            raise KvsClientException(f"ping failed: {res.status_code} {res.text}")

    def put(self, node: _NodeLike, key: str, value: str) -> PutResponse:
        id = self._new_id()
        log(f"client {self.name} [{id}] -> {node.name}: put {key!r} := {value!r}")
        if len(key) == 0:
            raise ValueError("key cannot be empty")
        res = self._request(id, node, "put", f"data/{key}", json={"value": value})
        return PutResponse(existed=res.status_code == 200, status_code=res.status_code, ok=res.ok)

    def get(self, node: _NodeLike, key: str) -> GetResponse:
        id = self._new_id()
        log(f"client {self.name} [{id}] -> {node.name}: get {key!r}")
        if len(key) == 0:
            raise ValueError("key cannot be empty")
        res = self._request(id, node, "get", f"data/{key}")
        if res.ok:
            value = res.json()["value"]
            log(f"client {self.name} [{id}] -> {node.name}: get {key!r} |> {value!r}")
        else:
            value = None
        return GetResponse(value=value, status_code=res.status_code, ok=True)

    def delete(self, node: _NodeLike, key: str) -> DeleteResponse:
        id = self._new_id()
        log(f"client {self.name} [{id}] -> {node.name}: delete {key!r}")
        if len(key) == 0:
            raise ValueError("key cannot be empty")
        res = self._request(id, node, "delete", f"data/{key}")
        return DeleteResponse(status_code=res.status_code, ok=res.ok)

    def get_all(self, node: _NodeLike) -> GetAllResponse:
        id = self._new_id()
        log(f"client {self.name} [{id}] -> {node.name}: list")
        res = self._request(id, node, "get", "data")
        if not res.ok:
            raise KvsClientException(f"list failed: {res.status_code} {res.text}")
        values = res.json()
        if not isinstance(values, dict):
            raise KvsClientException("list failed: expected a dict")
        log(f"client {self.name} [{id}] -> {node.name}: list |> [{len(values)} items]")
        return GetAllResponse(status_code=res.status_code, values=values, ok=res.ok)

    def send_view(self, node: _NodeLike, view: Sequence[_NodeLike]) -> None:
        id = self._new_id()
        view_ = [dict(address=f"{n.ip}:8081", id=n.index) for n in view]
        log(
            f"client {self.name} [{id}] -> {node.name}: view {[f'{n.name} (addr={n.ip}:8081, id={n.index})' for n in view]}"
        )
        res = self._request(id, node, "put", "view", json={"view": view_})
        if not res.ok:
            raise KvsClientException(f"send_view failed: {res.status_code} {res.text}")

    def broadcast_view(self, nodes: Sequence[_NodeLike]) -> None:
        log(f"client {self.name}: broadcast view")
        for node in nodes:
            self.send_view(node, nodes)
