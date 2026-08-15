from fastapi import APIRouter, HTTPException, Request, Body
from pydantic import BaseModel, Field
from fastapi.responses import JSONResponse
from typing import List, Dict, Optional, Tuple
from threading import Thread, Lock
from concurrent.futures import ThreadPoolExecutor, as_completed
from uhashring import HashRing

import os
import asyncio
import httpx
import requests
import json
import time
from hashlib import blake2b

router = APIRouter()

K = 2 << 512


def hashing(value: str):
    h = blake2b()
    h.update(value.encode())
    return int(h.hexdigest(), 16) % K


class HR:
    def __init__(self, shards: List, vnodes: int = 100):
        self.vnodes = vnodes
        self.shardHashes = {}
        self.vnode_map = {}
        for shard in shards:
            for v in range(self.vnodes):
                vnode_key = f"{shard}-vn{v}"
                vnode_hash = hashing(vnode_key)
                self.shardHashes[vnode_key] = vnode_hash
                self.vnode_map[vnode_key] = shard
        self.shardHashes = dict(
            sorted(self.shardHashes.items(), key=lambda item: item[1])
        )

    def __str__(self):
        return f"Ring: {self.shardHashes}"

    def get_node(self, key: str):
        global shardView
        key_hash = hashing(key)
        for shard, shard_hash in self.shardHashes.items():
            if shard_hash > key_hash and len(shardView[self.vnode_map[shard]]) > 0:
                return self.vnode_map[shard]
        return self.vnode_map[next(iter(self.shardHashes))]

    def remove_node(self, shardID: str):
        to_remove = [
            vnode for vnode in self.shardHashes if vnode.startswith(f"{shardID}-vn")
        ]
        for vnode in to_remove:
            del self.shardHashes[vnode]
            del self.vnode_map[vnode]
        self.shardHashes = dict(
            sorted(self.shardHashes.items(), key=lambda item: item[1])
        )

    def add_node(self, shardID: str):
        for v in range(self.vnodes):
            vnode_key = f"{shardID}-vn{v}"
            vnode_hash = hashing(vnode_key)
            self.shardHashes[vnode_key] = vnode_hash
            self.vnode_map[vnode_key] = shardID
        self.shardHashes = dict(
            sorted(self.shardHashes.items(), key=lambda item: item[1])
        )


class HRing:
    def __init__(self, shards: List):
        self.Ring = HR(shards)

    def add_node(self, shardID: str):
        self.Ring.add_node(shardID)

    def get_node(self, key: str):
        return self.Ring.get_node(key)

    def keys2move(self, myShard: str, items: dict):
        mappings = {
            key: node
            for key, node in ((key, self.Ring.get_node(key)) for key in items.keys())
            if node != myShard
        }
        return mappings


def get_node_index():
    global view, NODE_ID
    for idx, node in enumerate(view):
        if str(node.id) == str(NODE_ID):
            return idx


class VectorClock:
    def __init__(self):
        self.clock = [0 for _ in range(len(view))]

    def increment(self):
        id = get_node_index()
        self.clock[id] += 1

    def mergeClocks(self, other):
        if len(self.clock) != len(other.clock):
            return False
        self.clock = [max(a, b) for a, b in zip(self.clock, other.clock)]
        return True

    def __str__(self):
        return str(self.clock)

    def __lt__(self, other):
        if len(self.clock) != len(other.clock):
            return False
        less = False
        for a, b in zip(self.clock, other.clock):
            if a > b:
                return False
            if a <= b:
                less = True
        return less

    def __eq__(self, other):
        if len(self.clock) != len(other.clock):
            return False
        if (self < other) == (other < self):
            return True
        return False


# Class to manage the creation and modification of Causal Metadata JSON
# Add methods here as you see fit
class CausalMetadata:
    def __init__(self):
        self.data = {}

    def add_key(self, key: str, value: str, dependencies: dict):
        self.data[key] = (VectorClock(), time.time(), value, dependencies)

    def update_key(
        self, key: str, vc: VectorClock, timestamp: int, value: str, dependencies: dict
    ):
        self.data[key] = (vc, timestamp, value, dependencies)

    def to_json(self) -> str:
        seralized_data = {}
        for key, (vc, timestamp, value, dependencies) in self.data.items():
            seralized_data[key] = (
                vc.clock if isinstance(vc, VectorClock) else vc,
                timestamp,
                value,
                dependencies,
            )
        return json.dumps(seralized_data)

    def from_json(self, string: str):
        loaded = json.loads(string)
        self.data = {}
        for key, (clock_list, timestamp, value, dependencies) in loaded.items():
            vc = VectorClock()
            vc.clock = clock_list
            self.data[key] = (vc, timestamp, value, dependencies)

    def get_key(self, key: str):
        return self.data.get(key, None)

    def vc(self, key: str):
        return self.data[key][0]

    def timestamp(self, key: str):
        return self.data[key][1]

    def value(self, key: str):
        return self.data[key][2]

    def dependencies(self, key: str):
        return self.data[key][3]

    # Call will increment vector clock by one
    def update_vector_clock(self, key: str):
        data = self.get_key(key)
        if data is None:
            return False
        data[0].increment()
        self.data[key] = data
        return True

    def __str__(self):
        funString = "CausalMetadata:\n"
        for key, (vc, timestamp, value, dependencies) in self.data.items():
            vc_str = vc.clock if isinstance(vc, VectorClock) else vc
            funString += f"  Key: {key}\n"
            funString += f"    VectorClock: {vc_str}\n"
            funString += f"    Timestamp: {timestamp}\n"
            funString += f"    Value: {value}\n"
            funString += f"    Dependencies: {dependencies}\n"
        return funString

    def merge_data(self, self_id, other_data, other_id):
        # iterate through incoming metadata
        for key, (vc, timestamp, value, dependencies) in other_data.data.items():
            # if incoming data contains a key not in our current data, add it
            if key not in self.data:
                self.data[key] = (vc, timestamp, value, dependencies)
            # key is present in both
            else:
                # vector clocks are concurrent or equal, tiebreak using arbitration order
                if self.vc(key) == vc:
                    if self.timestamp(key) < timestamp:
                        self.data[key] = (vc, timestamp, value, dependencies)
                    elif self.timestamp(key) == timestamp and self_id < other_id:
                        self.data[key] = (vc, timestamp, value, dependencies)
                    # all other cases: timestamp of self exceeds other or timestamps are equal but own id exceeds other (keep own in both cases)
                # vector clocks are not concurrent
                else:
                    # if current vector clock is older than incoming, take incoming
                    if self.data[key][0] < vc:
                        self.data[key] = (vc, timestamp, value, dependencies)
                    # otherwise, keep current


# Create an instance of CausalMetadata


causal_data = CausalMetadata()
# In-memory key-value store
items = {}
# view for node
view = []
hr = None
# TODO: ShardView
shardView = {}
# Vector Clock For Ini
# Configuration of the current node
# Shard ID is "Unknown" if not set (should be set upon view init/view change)
SHARD_ID = "Unknown"
# Node ID is "Unknown" if not set
NODE_ID = os.getenv("NODE_IDENTIFIER", "Unknown")
NODE_ADDRESS = os.getenv("NODE_ADDRESS", "172.4.0.1:8081")
TIMEOUT = 5  # Constant to use for setting a timeout in any endpoints
BROADCASTSLEEP = 0.1
# Setting Low because we don't care about getting a response ever/don't expect a response?
GOSSIPTIMEOUT = 1
"""Request Tracking Variables for Gossip Protocol"""
pending_requests = 0
# Might not be neccesary but going to use to ensure counter is only be incremented one at a time and no race conditions occur with our endpoints
pending_requests_lock = asyncio.Lock()


class Item(BaseModel):
    value: str
    causal_metadata: Dict[str, Tuple[List[int], float, str]] = Field(
        ..., alias="causal-metadata"
    )


class GetBody(BaseModel):
    causal_metadata: Dict[str, Tuple[List[int], float, str]] = Field(
        ..., alias="causal-metadata"
    )


class Node(BaseModel):
    address: str
    id: str


class ViewBody(BaseModel):
    view: Dict[str, List[Node]]


class GetStuffBody(BaseModel):
    view: Dict[str, List[Node]]
    askingShard: str


# returns the address of the first node in the requested shard
def node_address_in_shard(shardID):
    return shardView[shardID][0].address


def find_keys_in_dependencies(target_key, causal_metadata):
    """
    Returns a list of keys in causal_metadata whose dependencies include target_key.
    """
    found = []
    for key, (_, _, _, dependencies) in causal_metadata.data.items():
        if target_key in dependencies:
            found.append(key)
    return found


# Gossip Process
async def broadcast_metadata():
    global view
    global pending_requests
    global causal_data

    if pending_requests > 0:
        return
    # I think we could have a request start at the same time as the broadcast happens but might be fine?

    jsonData = causal_data.to_json()

    async def send_message(node):
        if node.id != NODE_ID:
            try:
                async with httpx.AsyncClient() as client:
                    await client.put(
                        f"http://{node.address}/gossip",
                        json={"metadata": jsonData, "ID": NODE_ID},
                        timeout=GOSSIPTIMEOUT,
                    )
            except Exception as e:
                print(f"Error broadcasting to {node.id}: {e}")

    tasks = [asyncio.create_task(send_message(node)) for node in view]
    return


async def periodic_broadcast():
    while True:
        async with pending_requests_lock:
            if pending_requests == 0:
                await broadcast_metadata()
        # Wait for 100 MS before trying again
        await asyncio.sleep(BROADCASTSLEEP)


@router.put("/gossip")
async def gossip(request: Request):
    global causal_data, items
    data = await request.json()
    metadata = data.get("metadata")
    other_id = data.get("ID")
    incoming_metadata = CausalMetadata()
    incoming_metadata.from_json(metadata)
    if pending_requests == 0:
        async with pending_requests_lock:
            causal_data.merge_data(NODE_ID, incoming_metadata, other_id)
            for key, (_, _, value, _) in causal_data.data.items():
                responsible_shard = hr.get_node(key)
                if responsible_shard == SHARD_ID:
                    items[key] = value


@router.on_event("startup")
async def startup_event():
    asyncio.create_task(periodic_broadcast())


@router.get("/ping")
async def ping_node():
    return {"message": f"Node {NODE_ID} is Initialized"}
    # raise HTTPException(status_code=503, detail="View Uninitalized")


@router.post("/internal/receive_new_shard_keys")
async def receive_new_shard_keys(item: dict = Body(...)):
    global items, causal_data

    keys_data = item.get("keys", {})
    for key, value_dict in keys_data.items():
        value = value_dict["value"]
        metadata = value_dict.get("causal-metadata", {})
        items[key] = value
        if metadata:
            vc, timestamp, val, dependencies = metadata
            vc_obj = VectorClock()
            vc_obj.clock = vc
            causal_data.data[key] = (vc_obj, timestamp, val, dependencies)
    return JSONResponse(
        status_code=200, content={"message": "New shard keys received."}
    )


@router.post("/giveMeStuff")
async def sendKeys(view_body: GetStuffBody):
    global items, causal_data, hr, shardView
    asking_shard = view_body.askingShard
    old_view = shardView
    shardView = view_body.view

    hr = HRing(list(shardView.keys()))

    keys_for_asking_shard = [key for key in items if hr.get_node(key) == asking_shard]

    new_shards = [shard for shard in shardView if shard not in old_view]

    keys_for_new_shards = {}
    for shard in new_shards:
        keys_for_new_shards[shard] = [key for key in items if hr.get_node(key) == shard]

    data_to_send = {
        key: {
            "value": items[key],
            "causal-metadata": json.loads(causal_data.to_json()).get(key, None),
        }
        for key in keys_for_asking_shard
    }

    new_shard_data = {
        shard: {
            key: {
                "value": items[key],
                "causal-metadata": json.loads(causal_data.to_json()).get(key, None),
            }
            for key in keys_for_new_shards[shard]
        }
        for shard in new_shards
    }
    for shard, keys_dict in new_shard_data.items():
        # Pick the first node in the new shard to send the data
        node = view_body.view[shard][0]
        try:
            async with httpx.AsyncClient() as client:
                await client.post(
                    f"http://{node.address}/internal/receive_new_shard_keys",
                    json={"keys": keys_dict},
                    timeout=TIMEOUT,
                )
        except Exception as e:
            print(f"Failed to send new shard keys to {node.address}: {e}")

    return JSONResponse(status_code=200, content={"keys": data_to_send})


@router.put("/view")
async def update_view(view_body: ViewBody):
    global shardView, hr, items, causal_data, NODE_ID, SHARD_ID, view
    print(view_body)
    if not view_body:
        raise HTTPException(
            status_code=400, detail="Request body must contain shardView"
        )
    old_view = shardView
    shardView = view_body.view  # Update shardView dictionary (shardID -> list of Nodes)

    # Build consistent hash ring with shard IDs
    shard_ids = list(shardView.keys())
    hr = HRing(shard_ids)
    # Find which shard this node belongs to
    my_shard = None
    for shard_id, nodes in shardView.items():
        if any(node.id == NODE_ID for node in nodes):
            my_shard = shard_id
            SHARD_ID = shard_id
            view = nodes
            break

    if my_shard is None:
        raise HTTPException(
            status_code=400, detail="Current node's shard not found in view"
        )
    for shard_id in old_view:
        if shard_id not in shardView or len(shardView[shard_id]) == 0:
            hr.Ring.remove_node(shard_id)  # Remove from hash ring
    # Find keys that no longer belong to this shard (keys to move)
    keys_to_move = hr.keys2move(my_shard, items)

    # Helper to forward a key to its new shard
    async def forward_key(key, target_shard):
        nodes = shardView[target_shard]
        if len(nodes) == 0:
            return
        for node in nodes:
            try:
                async with httpx.AsyncClient() as client:
                    await client.put(
                        f"http://{node.address}/internal/data/{key}",
                        json={
                            "value": items[key],
                            "causal-metadata": causal_data.to_json(),
                        },
                        timeout=TIMEOUT,
                    )
                break  # Stop on first successful node
            except Exception as e:
                print(f"Failed to send key {key} to {node.address}: {e}")

    # Forward keys asynchronously
    forward_tasks = [forward_key(key, shard) for key, shard in keys_to_move.items()]
    await asyncio.gather(*forward_tasks)

    # Ask the other shards in the previous view to send this node some stuff
    for shardName, nodes in old_view.items():
        if shardName not in shardView.keys() or len(shardView[shardName]) == 0:
            node = nodes[0]
            try:
                async with httpx.AsyncClient() as client:
                    response = await client.post(
                        f"http://{node.address}/giveMeStuff",
                        json={
                            "view": {
                                shard: [
                                    node.dict() if hasattr(node, "dict") else dict(node)
                                    for node in nodes
                                ]
                                for shard, nodes in shardView.items()
                            },
                            "askingShard": SHARD_ID,
                        },
                        timeout=TIMEOUT,
                    )
                    response_json = response.json()
                    keys_data = response_json.get("keys", {})

                    for key, value_dict in keys_data.items():
                        value = value_dict["value"]
                        metadata = value_dict.get("causal-metadata", {})

                        items[key] = value
                        if metadata:
                            # If metadata is a dict/tuple for a single key:
                            vc, timestamp, val, dependencies = metadata
                            vc_obj = VectorClock()
                            vc_obj.clock = vc
                            causal_data.data[key] = (
                                vc_obj,
                                timestamp,
                                val,
                                dependencies,
                            )
            except Exception as e:
                print(f"Failed to send key {key} to {node.address}: {e}")

    # Remove moved keys locally
    for key, shard in keys_to_move.items():
        if len(shardView[shard]) != 0:
            items.pop(key, None)
            causal_data.data.pop(key, None)

    return JSONResponse(
        content={"message": "Shard view updated and keys forwarded."}, status_code=200
    )


@router.put("/internal/data/{key}")
async def internal_put_data(key: str, item: dict = Body(...)):
    global items, causal_data

    if "value" not in item or "causal-metadata" not in item:
        raise HTTPException(
            status_code=400, detail="Must provide 'value' and 'causal-metadata'"
        )

    value = item["value"]
    causal_metadata_json = item["causal-metadata"]

    # Load incoming causal metadata
    incoming_causal = CausalMetadata()
    incoming_causal.from_json(causal_metadata_json)

    # Update local store
    items[key] = value
    causal_data.data[key] = incoming_causal.data.get(
        key, causal_data.data.get(key, (VectorClock(), time.time(), value, {}))
    )

    return JSONResponse(
        content={"message": f"Key {key} stored internally."}, status_code=200
    )


@router.get("/data")
async def get_all_data(item: dict = Body(...)):
    global causal_data, items, view

    if not view:
        raise HTTPException(status_code=503, detail="View is uninitialized or empty")

    # /TODO: Is my endpoint

    if "causal-metadata" not in item:
        raise HTTPException(
            status_code=400,
            detail="Request body must contain 'causal-metadata'",
        )

    client_metadata = item["causal-metadata"]
    if isinstance(client_metadata, str):
        try:
            client_metadata = json.loads(client_metadata)
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid JSON format")

    if not isinstance(client_metadata, dict):
        raise HTTPException(status_code=400, detail="'causal-metadata' must be a dict")

    client_causal = CausalMetadata()
    client_causal.from_json(json.dumps(client_metadata))

    async def get_key_data(key):
        local_data = causal_data.get_key(key)
        if not local_data:
            local_data = client_causal.get_key(key)
            if not local_data:
                return None

        vc, ts, val, deps = local_data

        # Wait for deps to be satisfied
        while True:
            satisfied = True
            for dep_key, dep_vc in deps.items():
                responsible_shard = hr.get_node(dep_key)
                if responsible_shard != SHARD_ID:
                    continue
                if dep_key not in causal_data.data:
                    satisfied = False
                    break
                node_vc = causal_data.vc(dep_key)
                if isinstance(dep_vc, list):
                    dep_vc_obj = VectorClock()
                    dep_vc_obj.clock = dep_vc
                else:
                    dep_vc_obj = dep_vc
                if node_vc < dep_vc_obj and not node_vc == dep_vc_obj:
                    satisfied = False
                    break
            if satisfied:
                break
            await asyncio.sleep(0.05)

        return key, val, vc, ts, deps

    # Run all gets concurrently
    tasks = [get_key_data(key) for key in items]

    # Add keys in the clients metadata that are not in  items

    for key in client_metadata:
        responsible_shard = hr.get_node(key)
        if key not in items and responsible_shard == SHARD_ID:
            tasks.append(get_key_data(key))
    results = await asyncio.gather(*tasks)

    result_items = {}
    clock_accumulator = {}

    for result in results:
        if result is None:
            continue
        key, val, vc, ts, deps = result
        result_items[key] = val

        # Prepare max vector clock merging
        if key not in clock_accumulator:
            clock_accumulator[key] = vc.clock.copy()
        else:
            clock_accumulator[key] = [
                max(a, b) for a, b in zip(clock_accumulator[key], vc.clock)
            ]

    # Final merge AFTER all get operations
    for key, clock in clock_accumulator.items():
        merged_vc = VectorClock()
        merged_vc.clock = clock
        local_data = causal_data.get_key(key)
        if local_data:
            ts, val, deps = local_data[1], local_data[2], local_data[3]
            client_causal.update_key(key, merged_vc, ts, val, deps)

            # Also add dependencies (safe)
            for dep_key, dep_vc in deps.items():
                if dep_key in causal_data.data:
                    client_causal.update_key(
                        dep_key,
                        causal_data.vc(dep_key),
                        causal_data.timestamp(dep_key),
                        causal_data.value(dep_key),
                        causal_data.dependencies(dep_key),
                    )
                else:
                    client_causal.update_key(dep_key, dep_vc, 0, None, {})

    return JSONResponse(
        status_code=200,
        content={
            "items": result_items,
            "causal-metadata": json.loads(client_causal.to_json()),
        },
    )


@router.post("/internalGet/{key}")
async def get_internal(
    key: str,
    item: dict = Body(...),
):
    global hr, SHARD_ID, causal_data, items, view
    if not view:
        raise HTTPException(status_code=503, detail="View is uninitialized or empty")
    if "causal-metadata" not in item:
        raise HTTPException(
            status_code=400,
            detail="Request body must contain 'causal-metadata' ",
        )
    client_metadata = item["causal-metadata"]
    if isinstance(client_metadata, str):
        try:
            client_metadata = json.loads(client_metadata)
        except Exception:
            raise HTTPException(
                status_code=400,
                detail="Invalid JSON string for 'causal-metadata'.",
            )
    if not isinstance(client_metadata, dict):
        raise HTTPException(
            status_code=400,
            detail="'causal-metadata' must be a dict or a JSON string representing a dict.",
        )

    client_causal = CausalMetadata()
    client_causal.from_json(json.dumps(client_metadata))
    key_client_data = client_causal.get_key(key)
    key_local_data = causal_data.get_key(key)

    if key_local_data is None and key_client_data is None:
        raise HTTPException(status_code=404, detail="Not Found")
    if key_client_data is not None:
        dependencies = (
            client_causal.dependencies(key) if key in client_causal.data else {}
        )

        def deps_satisfied():
            for dep_key, dep_vc in dependencies.items():
                responsible_shard = hr.get_node(dep_key)
                if responsible_shard != SHARD_ID:
                    continue
                # If key doesn't exist in node then automatically not satisifed
                if dep_key not in causal_data.data:
                    return False
                node_vc = causal_data.vc(dep_key)
                if isinstance(dep_vc, list):
                    dep_vc_obj = VectorClock()
                    dep_vc_obj.clock = dep_vc
                else:
                    dep_vc_obj = dep_vc  # Already a VectorClock
                if node_vc < dep_vc_obj and not node_vc == dep_vc_obj:
                    return False
            return True

        while (
            key_local_data is None
            or (
                causal_data.vc(key) < key_client_data[0]
                and not causal_data.vc(key) == key_client_data[0]
            )
            or not deps_satisfied()
        ):
            await asyncio.sleep(0.1)
            print("oops I did it again")
            key_local_data = causal_data.get_key(key)
            dependencies = (
                causal_data.dependencies(key) if key in causal_data.data else {}
            )
    # if client data is in the causal history of the local data, we return the value and pass updated causal data?
    # Is this sufficient for causal consistency,? Or do we need to check all keys vector clocks on a GET?
    if (
        key_client_data is None
        or key_client_data[0] < key_local_data[0]
        or (
            not key_client_data[0] < key_local_data[0]
            and not key_local_data[0] < key_client_data[0]
        )
    ):
        # TODO:Here is where dependency stuff should be checked as well
        client_causal.update_key(
            key,
            causal_data.vc(key),
            causal_data.timestamp(key),
            causal_data.value(key),
            causal_data.dependencies(key),
        )
        for dep_key, dep_vc in causal_data.dependencies(key).items():
            if dep_key in causal_data.data:
                client_causal.update_key(
                    dep_key,
                    causal_data.vc(dep_key),
                    causal_data.timestamp(dep_key),
                    causal_data.value(dep_key),
                    causal_data.dependencies(dep_key),
                )
            else:
                client_causal.update_key(dep_key, dep_vc, 0, None, {})
        data = client_causal.to_json()
        return JSONResponse(
            status_code=200, content={"value": items[key], "causal-metadata": data}
        )
    raise HTTPException(status_code=404, detail="Not Found")


# As is will return a 422 error on invalid format, this might not work with their tests?
@router.get("/data/{key}")
async def get_data(
    key: str,
    item: dict = Body(...),
):
    global hr, SHARD_ID, causal_data, items, view, causal

    if not view:
        raise HTTPException(status_code=503, detail="View is uninitialized or empty")
    shard_id = hr.get_node(key)
    if shard_id != SHARD_ID:
        node_address = node_address_in_shard(shard_id)
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"http://{node_address}/internalGet/{key}",
                    json=item,
                    timeout=TIMEOUT,
                )
                return JSONResponse(
                    status_code=response.status_code, content=response.json()
                )
        except Exception:
            while True:
                await asyncio.sleep(15)

    else:
        if not view:
            raise HTTPException(
                status_code=503, detail="View is uninitialized or empty"
            )
        if "causal-metadata" not in item:
            raise HTTPException(
                status_code=400,
                detail="Request body must contain 'causal-metadata' ",
            )
        client_metadata = item["causal-metadata"]
        if isinstance(client_metadata, str):
            try:
                client_metadata = json.loads(client_metadata)
            except Exception:
                raise HTTPException(
                    status_code=400,
                    detail="Invalid JSON string for 'causal-metadata'.",
                )
        if not isinstance(client_metadata, dict):
            raise HTTPException(
                status_code=400,
                detail="'causal-metadata' must be a dict or a JSON string representing a dict.",
            )

        client_causal = CausalMetadata()
        client_causal.from_json(json.dumps(client_metadata))
        key_client_data = client_causal.get_key(key)
        key_local_data = causal_data.get_key(key)

        if key_local_data is None and key_client_data is None:
            raise HTTPException(status_code=404, detail="Not Found")
        if key_client_data is not None:
            # def deps_satisfied():
            #     for dep_key, dep_vc in dependencies.items():
            #         responsible_shard = hr.get_node(dep_key)
            #         if responsible_shard != SHARD_ID:
            #             continue
            #         # If key doesn't exist in node then automatically not satisifed
            #         if dep_key not in causal_data.data:
            #             return False
            #         node_vc = causal_data.vc(dep_key)
            #         if isinstance(dep_vc, list):
            #             dep_vc_obj = VectorClock()
            #             dep_vc_obj.clock = dep_vc
            #         else:
            #             dep_vc_obj = dep_vc  # Already a VectorClock
            #         if node_vc < dep_vc_obj and not node_vc == dep_vc_obj:
            #             return False
            #     return True

            while key_local_data is None or (
                causal_data.vc(key) < key_client_data[0]
                and not causal_data.vc(key) == key_client_data[0]
            ):
                await asyncio.sleep(0.1)
                key_local_data = causal_data.get_key(key)

        # if client data is in the causal history of the local data, we return the value and pass updated causal data?
        # Is this sufficient for causal consistency,? Or do we need to check all keys vector clocks on a GET?
        if (
            key_client_data is None
            or key_client_data[0] < key_local_data[0]
            or (
                not key_client_data[0] < key_local_data[0]
                and not key_local_data[0] < key_client_data[0]
            )
        ):
            # TODO:Here is where dependency stuff should be checked as well
            client_causal.update_key(
                key,
                causal_data.vc(key),
                causal_data.timestamp(key),
                causal_data.value(key),
                causal_data.dependencies(key),
            )
            for dep_key, dep_vc in causal_data.dependencies(key).items():
                if dep_key in causal_data.data:
                    client_causal.update_key(
                        dep_key,
                        causal_data.vc(dep_key),
                        causal_data.timestamp(dep_key),
                        causal_data.value(dep_key),
                        causal_data.dependencies(dep_key),
                    )
                else:
                    client_causal.update_key(dep_key, dep_vc, 0, None, {})
            data = client_causal.to_json()
            return JSONResponse(
                status_code=200, content={"value": items[key], "causal-metadata": data}
            )
        raise HTTPException(status_code=404, detail="Not Found")
        # Repeat above?


@router.put("/data/{key}")
async def put_data(key: str, item: dict = Body(...)):
    global hr, SHARD_ID, causal_data, items, view

    if not view:
        raise HTTPException(status_code=503, detail="View is uninitialized or empty")
    shard_id = hr.get_node(key)

    if shard_id != SHARD_ID:
        node_address = node_address_in_shard(shard_id)
        try:
            async with httpx.AsyncClient() as client:
                response = await client.put(
                    f"http://{node_address}/data/{key}",
                    json=item,
                    timeout=TIMEOUT,
                )
                return JSONResponse(
                    status_code=response.status_code, content=response.json()
                )
        except Exception:
            while True:
                await asyncio.sleep(15)

    else:
        # Validate request body
        if (
            "value" not in item
            or not isinstance(item["value"], str)
            or "causal-metadata" not in item
        ):
            raise HTTPException(
                status_code=400,
                detail="Request body must contain 'value' (str) and 'causal-metadata' (dict)",
            )

        value = item["value"]
        client_metadata = item["causal-metadata"]
        if isinstance(client_metadata, str):
            try:
                client_metadata = json.loads(client_metadata)
            except Exception:
                raise HTTPException(
                    status_code=400,
                    detail="Invalid JSON string for 'causal-metadata'.",
                )
        if not isinstance(client_metadata, dict):
            raise HTTPException(
                status_code=400,
                detail="'causal-metadata' must be a dict or a JSON string representing a dict.",
            )
        # Load the client's metadata into a temporary CausalMetadata instance
        client_causal = CausalMetadata()
        client_causal.from_json(json.dumps(client_metadata))
        vc = None

        # Dependencies are all of the keys in the clients causal history
        dependencies = {
            dep_key: client_metadata[dep_key][0]
            for dep_key in client_metadata.keys()
            if dep_key != key
        }
        # Handle local key update or creation
        if key not in causal_data.data:
            # New key — initialize vector clock
            vc = VectorClock()
            vc.increment()
            timestamp = time.time()
            causal_data.data[key] = (vc, timestamp, value, dependencies)
            operation_status = "created"
        else:
            # Existing key — update value and increment
            vc, _, _, dependencies = causal_data.data[key]
            vc.increment()
            timestamp = time.time()
            causal_data.data[key] = (vc, timestamp, value, dependencies)
            operation_status = "updated"

        # Store key in items (value only)
        items[key] = value

        # Append/Update the new metadata to the client's causal metadata
        client_causal.data[key] = (vc.clock.copy(), timestamp, value, dependencies)

        # Return the response with causal metadata and operation status
        return JSONResponse(
            status_code=200,
            content={
                "causal-metadata": json.loads(client_causal.to_json()),
                "message": f"Key '{key}' has been {operation_status}.",
            },
        )
