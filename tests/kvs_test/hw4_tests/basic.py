import json
from multiprocessing.pool import ThreadPool

from ..containers import ClusterConductor
from ..hw4_api import KvsFixture
from ..testcase import TestCase
from ..util import log
import random
import string


def basic_put_get_with_metadata(conductor: ClusterConductor, fx: KvsFixture):
    """Test basic put and get operations with causal metadata."""
    nodes = conductor.alternative_spawn_cluster([[3], [3], [3]])
    client = fx.create_client(name="client1")
    client.broadcast_view(nodes)

    log("\n> TEST: Basic Put/Get with Causal Metadata")

    # Initial causal metadata (can be empty or None depending on client design)
    causal_metadata = {}

    # Put a value with causal metadata
    r = client.put(nodes["shard1"][0], "test_key", "test_value", causal_metadata=causal_metadata)
    print(r)
    assert r["status_code"] == 200, f"expected 200 for new key, got {r['status_code']}"
    causal_metadata = r["causal_metadata"]  # update causal metadata from response
    # Get the value with causal metadata
    r = client.get(nodes["shard1"][0], "test_key", causal_metadata=causal_metadata)
    print(r)
    assert r["status_code"] == 200, f"expected 200 for get, got {r['status_code']}"
    assert r["value"] == "test_value", f"expected 'test_value', got '{r['value']}'"
    assert r["causal_metadata"] is not None, "expected causal metadata in GET response"

    return True, "OK"


def basic_causal_chain(conductor: ClusterConductor, fx: KvsFixture):
    """Test a simple causal chain: put A -> get A -> put B -> get B."""
    nodes = conductor.spawn_cluster(node_count=9, nodes_per_shard=3)
    client = fx.create_client(name="client1")
    client.broadcast_view(nodes)
    causal_metadata = {}

    log("\n> TEST: Basic Causal Chain")

    # Put key1 -> value1
    r1 = client.put(nodes["shard0"][0], "key1", "value1", causal_metadata=causal_metadata)
    assert r1["status_code"] == 200, f"expected 200 for PUT key1, got {r1['status_code']}"
    cm1 = r1["causal_metadata"]

    # Get key1
    r2 = client.get(nodes["shard1"][0], "key1", causal_metadata=cm1)
    assert r2["status_code"] == 200, f"expected 200 for GET key1, got {r2['status_code']}"
    assert r2["value"] == "value1", f"expected 'value1', got '{r2['value']}'"
    cm2 = r2["causal_metadata"]

    # Put key2 -> value2 (causally after reading key1)
    r3 = client.put(nodes["shard1"][0], "key2", "value2", causal_metadata=cm2)
    assert r3["status_code"] == 200, f"expected 200 for PUT key2, got {r3['status_code']}"
    cm3 = r3["causal_metadata"]

    # Get key2
    r4 = client.get(nodes["shard1"][0], "key2", causal_metadata=cm3)
    assert r4["status_code"] == 200, f"expected 200 for GET key2, got {r4['status_code']}"
    assert r4["value"] == "value2", f"expected 'value2', got '{r4['value']}'"

    return True, "OK"


def random_string(length=8):
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=length))


def test_shard_disjoint_get_all(conductor: ClusterConductor, fx: KvsFixture):
    """Put random keys on each shard, then check get_all returns disjoint sets."""
    num_shards = 3
    nodes_per_shard = 3
    keys_per_shard = 5

    nodes = conductor.spawn_cluster(node_count=num_shards * nodes_per_shard, nodes_per_shard=nodes_per_shard)
    client = fx.create_client(name="client_disjoint")
    client.broadcast_view(nodes)

    # Map: shard_name -> [node, ...]
    shard_names = list(nodes.keys())
    all_keys = {}

    # Put random keys on each shard
    for shard_name in shard_names:
        shard_nodes = nodes[shard_name]
        shard_keys = []
        for _ in range(keys_per_shard):
            key = f"{shard_name}_{random_string(6)}"
            value = random_string(10)
            r = client.put(shard_nodes[0], key, value, causal_metadata={})
            assert r["status_code"] == 200, f"PUT failed for {key} on {shard_name}"
            shard_keys.append(key)
        all_keys[shard_name] = set(shard_keys)

    # Do get_all on each shard and collect returned keys
    shard_keysets = {}
    for shard_name in shard_names:
        shard_nodes = nodes[shard_name]
        r = client.get_all(shard_nodes[0], causal_metadata={})
        assert r["status_code"] == 200, f"GET_ALL failed on {shard_name}"
        items = r.get("items", {})
        shard_keysets[shard_name] = set(items.keys())
    # Check that key sets are disjoint
    for i, s1 in enumerate(shard_names):
        for s2 in shard_names[i + 1 :]:
            overlap = shard_keysets[s1] & shard_keysets[s2]
            assert not overlap, f"Shards {s1} and {s2} have overlapping keys: {overlap}"

    print("Shard get_all key sets are disjoint:", {k: list(v) for k, v in shard_keysets.items()})
    return True, "All shards returned disjoint key sets in get_all"


def get_all_basic(conductor: ClusterConductor, fx: KvsFixture):
    """
    Test: Client puts two values, then does a get-all request.
    """
    nodes = conductor.spawn_cluster(node_count=3, nodes_per_shard=3)
    client = fx.create_client(name="client1")
    client.broadcast_view(nodes)

    log("\n> TEST: Get All Basic")

    causal_metadata = {}

    # Put two values
    r1 = client.put(nodes["shard0"][0], "x", "1", causal_metadata=causal_metadata)
    assert r1["status_code"] == 200, f"expected 200 for PUT x, got {r1['status_code']}"
    cm1 = r1["causal_metadata"]

    r2 = client.put(nodes["shard0"][0], "foo", "bar", causal_metadata=cm1)
    assert r2["status_code"] == 200, f"expected 200 for PUT foo, got {r2['status_code']}"
    cm2 = r2["causal_metadata"]

    # Get all
    r3 = client.get_all(nodes["shard0"][0], causal_metadata=cm2)
    assert r3["status_code"] == 200, f"expected 200 for GET ALL, got {r3['status_code']}"
    print(r3)
    assert "items" in r3, "expected 'items' in GET ALL response"
    items = r3["items"]
    assert items.get("x") == "1", f"expected x=1 in items, got {items}"
    assert items.get("foo") == "bar", f"expected foo=bar in items, got {items}"
    assert "causal-metadata" in r3, "expected 'causal-metadata' in GET ALL response"

    return True, "Get all endpoint returns all items and metadata"


def basic_availability(conductor: ClusterConductor, fx: KvsFixture):
    """Test basic availability: each node should be able to serve requests."""
    nodes = conductor.spawn_cluster(node_count=15, nodes_per_shard=3)
    client = fx.create_client(name="client1")
    client.broadcast_view(nodes)

    log("\n> TEST: Basic Availability")

    # Initial causal metadata (could be empty or None based on client implementation)
    causal_metadata = {}

    # Put a value through node 0
    r = client.put(nodes["shard0"][0], "avail_key", "avail_value", causal_metadata=causal_metadata)
    assert r["status_code"] == 200, f"expected 200 for PUT, got {r['status_code']}"
    causal_metadata = r["causal_metadata"]  # capture metadata for GETs
    shard_names = list(nodes.keys())
    # Try to get the value from all nodes using latest causal metadata
    for i in shard_names:
        r = client.get(nodes[i][0], "avail_key", causal_metadata=causal_metadata)
        assert r["status_code"] == 200, f"expected 200 for GET from node {i}, got {r['status_code']}"
        assert r["value"] == "avail_value", f"expected 'avail_value' from node {i}, got '{r['value']}'"
        assert r["causal_metadata"] is not None, f"missing causal metadata in GET response from node {i}"

    return True, "OK"


def initial_view_empty(conductor: ClusterConductor, fx: KvsFixture):
    """Test that nodes with empty views respond with 503, and work after view is set."""
    nodes = conductor.spawn_cluster(node_count=1, nodes_per_shard=1)
    client = fx.create_client(name="client1")

    log("\n> TEST: Initial View Empty")

    # Initial causal metadata (empty)
    causal_metadata = {}

    # Try to put a value before setting the view (should fail with 503)
    r = client.put(nodes["shard0"][0], "test_key", "test_value", causal_metadata=causal_metadata)
    assert r["status_code"] == 503, f"expected 503 before view is set, got {r['status_code']}"

    # Set the view
    client.broadcast_view(nodes)

    # Try to put again (should now succeed)
    r = client.put(nodes["shard0"][0], "test_key", "test_value", causal_metadata=causal_metadata)
    assert r["status_code"] == 200, f"expected 200 after setting view, got {r['status_code']}"

    return True, "OK"


def partitioned_three_clients_visibility(conductor: ClusterConductor, fx: KvsFixture):
    """
    Test: 2 shards, 3 clients, partitioned.
    - Client 2 puts x=1 on shard0.
    - Client 1 puts y=2 on shard0.
    - Client 3 gets y from shard0 (should get 2).
    - Client 3 gets x from shard1 (should get 404).
    """
    # 2 shards, 3 nodes per shard
    nodes = conductor.spawn_cluster(node_count=2, nodes_per_shard=2)
    client1 = fx.create_client(name="client1")
    client2 = fx.create_client(name="client2")
    client3 = fx.create_client(name="client3")
    client1.broadcast_view(nodes)
    client2.broadcast_view(nodes)
    client3.broadcast_view(nodes)

    # Partition the nodes so they cannot communicate
    # Partition 0: all nodes in shard0, Partition 1: all nodes in shard1
    conductor.create_partition(nodes["shard0"], "p0")

    log("\n> TEST: Partitioned Three Clients Visibility")

    cm1 = {}
    cm2 = {}
    cm3 = {}

    # Client 2 puts x=1 on shard0
    r1 = client2.put(nodes["shard0"][0], "x", "1", causal_metadata=cm2)
    assert r1["status_code"] == 200, f"expected 200 for client2 PUT x, got {r1['status_code']}"
    cm2 = r1["causal_metadata"]

    # Client 1 puts y=2 on shard0
    r2 = client1.put(nodes["shard0"][0], "y", "2", causal_metadata=cm1)
    assert r2["status_code"] == 200, f"expected 200 for client1 PUT y, got {r2['status_code']}"
    cm1 = r2["causal_metadata"]

    # Client 3 gets y from shard0 (should get 2)
    r3 = client3.get(nodes["shard0"][0], "y", causal_metadata=cm3)
    assert r3["status_code"] == 200, f"expected 200 for client3 GET y, got {r3['status_code']}"
    assert r3["value"] == "2", f"expected value '2' for y, got {r3['value']}"
    cm3 = r3["causal_metadata"]

    # Client 3 gets x from shard1 (should get 404)
    r4 = client3.get(nodes["shard0"][1], "x", causal_metadata=cm3)
    assert r4["status_code"] == 404, f"expected 404 for client3 GET x on shard1, got {r4['status_code']}"

    return True, "Partitioned three clients visibility test passed"


def multiple_clients_causal_metadata(conductor: ClusterConductor, fx: KvsFixture):
    """Test that causal metadata is correctly handled between multiple clients."""
    import time

    nodes = conductor.spawn_cluster(node_count=3, nodes_per_shard=3)
    client1 = fx.create_client(name="client1")
    client2 = fx.create_client(name="client2")
    client1.broadcast_view(nodes)
    client2.broadcast_view(nodes)
    nodes = nodes["shard0"]

    log("\n> TEST: Multiple Clients with Causal Metadata")
    cm0 = {}
    # Client 1 puts a value
    r = client1.put(nodes[0], "shared_key", "value1", causal_metadata=cm0)
    assert r["status_code"] == 200, f"expected 200 from client1 PUT value1, got {r['status_code']}"
    cm1 = r["causal_metadata"]

    # Client 1 gets the value
    r = client1.get(nodes[1], "shared_key", causal_metadata=cm1)
    assert r["status_code"] == 200, f"expected 200 from client1 GET, got {r['status_code']}"
    assert r["value"] == "value1", f"expected 'value1', got '{r['value']}'"
    cm2 = r["causal_metadata"]

    # Client 1 updates the value
    r = client1.put(nodes[2], "shared_key", "value2", causal_metadata=cm2)
    assert r["status_code"] == 200, f"expected 200 from client1 PUT value2, got {r['status_code']}"
    cm3 = r["causal_metadata"]

    # Client 2 tries to get the updated value (value2), retry with backoff
    max_retries = 10
    cm4 = {}
    for i in range(max_retries):
        r = client2.get(nodes[0], "shared_key", causal_metadata=cm4)
        if r["status_code"] == 200 and r["value"] == "value2":
            break
        time.sleep(0.5)
    assert r["value"] == "value2", f"Client 2 expected 'value2', got '{r['value']}'"
    cm5 = r["causal_metadata"]

    # Client 2 updates the value to value3
    r = client2.put(nodes[1], "shared_key", "value3", causal_metadata=cm5)
    assert r["status_code"] == 200, f"expected 200 from client2 PUT value3, got {r['status_code']}"
    cm6 = r["causal_metadata"]

    # Client 1 tries to get the updated value (value3), retry with backoff
    for i in range(max_retries):
        r = client1.get(nodes[2], "shared_key", causal_metadata=cm6)
        if r["status_code"] == 200 and r["value"] == "value3":
            break
        time.sleep(0.5)
    assert r["value"] == "value3", f"Client 1 expected 'value3', got '{r['value']}'"

    return True, "OK"


def inconsistent_causal_history_1(conductor: ClusterConductor, fx: KvsFixture):
    """
    Test: Two clients, two nodes, partitioned.
    - Client 1 puts x=1 to node 1.
    - Client 2 gets x from node 1 (should get 1)
    - Client 2 puts y=2 to node 2
    - Client 1 gets y from node 2 (should get 2)
    - Client 2 gets x from node 2 (should hang)
    """
    nodes = conductor.spawn_cluster(node_count=2, nodes_per_shard=2)
    client1 = fx.create_client(name="client1")
    client2 = fx.create_client(name="client2")
    client1.broadcast_view(nodes)
    client2.broadcast_view(nodes)
    nodes = nodes["shard0"]

    conductor.create_partition([nodes[0]], "p0")
    conductor.create_partition([nodes[1]], "p1")

    log("\n> TEST: Partitioned Puts and Gets")

    cm1 = {}
    cm2 = {}

    # Client 1 puts x=1 to node 1
    r1 = client1.put(nodes[0], "x", "1", causal_metadata=cm1)
    assert r1["status_code"] == 200, f"expected 200 for client1 PUT x, got {r1['status_code']}"
    cm1 = r1["causal_metadata"]
    #  Client 2 gets x from node 1 (should get 1)
    r2 = client2.get(nodes[0], "x", causal_metadata=cm2)
    assert r2["status_code"] == 200, f"expected 200 for client2 GET x, got {r2['status_code']}"
    assert r2["value"] == "1", f"expected value '1' for x, got {r2['value']}"
    cm2 = r2["causal_metadata"]
    # Client 2 puts y=2 to node 2
    r3 = client2.put(nodes[1], "y", "2", causal_metadata=cm2)
    assert r3["status_code"] == 200, f"expected 200 for client2 PUT y, got {r3['status_code']}"
    cm2 = r3["causal_metadata"]
    # Client 1 gets y from node 2 (should get 2)
    r4 = client1.get(nodes[1], "y", causal_metadata=cm2)
    assert r4["status_code"] == 200, f"expected 200 for client1 GET y, got {r4['status_code']}"
    assert r4["value"] == "2", f"expected value '2' for y, got {r4['value']}"
    cm1 = r4["causal_metadata"]
    # Client 2 gets x from node 2 (should hang)
    r5 = client2.get(nodes[1], "x", causal_metadata=cm2)
    assert r5["status_code"] == 408, f"expected timeout, got {r5['status_code']}"

    return True, "Server hangs when a previously read value doesn't exist in its KVS"


def inconsistent_causal_history_2(conductor: ClusterConductor, fx: KvsFixture):
    """
    Test: Two clients, two nodes, partitioned.
    - Client 1 puts x=1 to node 1.
    - Client 1 puts y=2 to node 1.
    - Client 2 gets y from node 1 (should get 2)
    - Client 2 gets x from node 2 (should hang)
    """
    nodes = conductor.spawn_cluster(node_count=2, nodes_per_shard=2)
    client1 = fx.create_client(name="client1")
    client2 = fx.create_client(name="client2")
    client1.broadcast_view(nodes)
    client2.broadcast_view(nodes)
    nodes = nodes["shard0"]

    conductor.create_partition([nodes[0]], "p0")
    conductor.create_partition([nodes[1]], "p1")

    log("\n> TEST: Partitioned Puts and Gets")

    cm1 = {}
    cm2 = {}

    # Client 1 puts x=1 to node 1
    r1 = client1.put(nodes[0], "x", "1", causal_metadata=cm1)
    assert r1["status_code"] == 200, f"expected 200 for client1 PUT x, got {r1['status_code']}"
    cm1 = r1["causal_metadata"]
    # Client 1 puts y=2 to node 1.
    r2 = client1.put(nodes[0], "y", "2", causal_metadata=cm1)
    assert r2["status_code"] == 200, f"expected 200 for client1 PUT y, got {r2['status_code']}"
    cm1 = r2["causal_metadata"]
    # Client 2 gets y from node 1 (should get 2)
    r3 = client2.get(nodes[0], "y", causal_metadata=cm2)
    assert r3["status_code"] == 200, f"expected 200 for client2 GET y, got {r3['status_code']}"
    assert r3["value"] == "2", f"expected value '2' for y, got {r3['value']}"
    cm1 = r3["causal_metadata"]
    # Client 2 gets x from node 2 (should hang)
    r5 = client2.get(nodes[1], "x", causal_metadata=cm2)
    assert r5["status_code"] == 408, f"expected timeout, got {r5['status_code']}"

    return True, "Server hangs when a previously read value doesn't exist in its KVS"


def partitioned_puts_and_gets(conductor: ClusterConductor, fx: KvsFixture):
    """
    Test: Two clients, two nodes, partitioned.
    - Client 1 puts x=1 to node 1.
    - Client 2 puts y=2 to node 2.
    - Client 1 gets y from node 2 (should get 2).
    - Client 2 gets x from node 2 (should get 404).
    """
    nodes = conductor.spawn_cluster(node_count=2, nodes_per_shard=2)
    client1 = fx.create_client(name="client1")
    client2 = fx.create_client(name="client2")
    client1.broadcast_view(nodes)
    client2.broadcast_view(nodes)
    nodes = nodes["shard0"]

    conductor.create_partition([nodes[0]], "p0")
    conductor.create_partition([nodes[1]], "p1")

    log("\n> TEST: Partitioned Puts and Gets")

    cm1 = {}
    cm2 = {}

    # Client 1 puts x=1 to node 1
    r1 = client1.put(nodes[0], "x", "1", causal_metadata=cm1)
    assert r1["status_code"] == 200, f"expected 200 for client1 PUT x, got {r1['status_code']}"
    cm1 = r1["causal_metadata"]
    # Client 2 puts y=2 to node 2
    r2 = client2.put(nodes[1], "y", "2", causal_metadata=cm2)
    assert r2["status_code"] == 200, f"expected 200 for client2 PUT y, got {r2['status_code']}"
    cm2 = r2["causal_metadata"]
    # Client 1 gets y from node 2 (should get value 2)
    r3 = client1.get(nodes[1], "y", causal_metadata=cm1)
    assert r3["status_code"] == 200, f"expected 200 for client1 GET y, got {r3['status_code']}"
    assert r3["value"] == "2", f"expected value '2' for y, got {r3['value']}"
    # Client 2 gets x from node 2 (should get 404)
    r4 = client2.get(nodes[1], "x", causal_metadata=cm2)
    assert r4["status_code"] == 404, f"expected 404 for client2 GET x, got {r4['status_code']}"

    return True, "Partitioned put/get test passed"


# Similar to above but ensures that clients interacting with some key y don't get causal metadata ifnromation about another key x
def partitioned_sequential_puts_and_gets(conductor: ClusterConductor, fx: KvsFixture):
    """
    Test: Two clients, two nodes, partitioned.
    - Client 1 puts y=1 to node 1.
    - Client 1 puts x=2 to node 1.
    - Client 2 gets y from node 1 (should get 1).
    - Client 2 gets x from node 2 (should get 404).
    """
    nodes = conductor.spawn_cluster(node_count=2, nodes_per_shard=2)
    client1 = fx.create_client(name="client1")
    client2 = fx.create_client(name="client2")
    client1.broadcast_view(nodes)
    client2.broadcast_view(nodes)
    nodes = nodes["shard0"]

    conductor.create_partition([nodes[0]], "p0")
    conductor.create_partition([nodes[1]], "p1")

    log("\n> TEST: Partitioned Sequential Puts and Gets")

    cm1 = {}
    cm2 = {}

    # Client 1 puts y=1 to node 1
    r1 = client1.put(nodes[0], "y", "1", causal_metadata=cm1)
    assert r1["status_code"] == 200, f"expected 200 for client1 PUT y, got {r1['status_code']}"
    cm1 = r1["causal_metadata"]
    # Client 1 puts x=2 to node 1
    r2 = client1.put(nodes[0], "x", "2", causal_metadata=cm1)
    assert r2["status_code"] == 200, f"expected 200 for client1 PUT x, got {r2['status_code']}"
    cm1 = r2["causal_metadata"]
    # Client 2 gets y from node 1 (should get value 1)
    r3 = client2.get(nodes[0], "y", causal_metadata=cm2)
    assert r3["status_code"] == 200, f"expected 200 for client2 GET y, got {r3['status_code']}"
    assert r3["value"] == "1", f"expected value '1' for y, got {r3['value']}"
    # Client 2 gets x from node 2 (should get 404)
    r4 = client2.get(nodes[1], "x", causal_metadata=cm2)
    print(r4)
    assert r4["status_code"] == 404, f"expected 404 for client2 GET x, got {r4['status_code']}"

    return True, "Partitioned sequential put/get test passed"


def concurrent_puts_same_key(conductor: ClusterConductor, fx: KvsFixture):
    """
    Test: Two clients, two nodes.
    - Client 1 puts x=1 to node 1.
    - Client 2 puts x=2 to node 2 (concurrently).
    - After both resolve, client 1 gets x from node 1 (should get 200 and value 1 or 2).
    """
    import threading

    nodes = conductor.spawn_cluster(node_count=2, nodes_per_shard=2)
    client1 = fx.create_client(name="client1")
    client2 = fx.create_client(name="client2")
    client1.broadcast_view(nodes)
    client2.broadcast_view(nodes)
    nodes = nodes["shard0"]

    log("\n> TEST: Concurrent PUTs to Same Key")

    cm1 = {}
    cm2 = {}

    # Define the two concurrent PUTs
    result1 = {}
    result2 = {}

    def put1():
        r = client1.put(nodes[0], "x", "1", causal_metadata=cm1)
        result1.update(r)

    def put2():
        r = client2.put(nodes[1], "x", "2", causal_metadata=cm2)
        result2.update(r)

    t1 = threading.Thread(target=put1)
    t2 = threading.Thread(target=put2)
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    assert result1["status_code"] == 200, f"expected 200 for client1 PUT, got {result1['status_code']}"
    assert result2["status_code"] == 200, f"expected 200 for client2 PUT, got {result2['status_code']}"

    # Use the latest causal metadata from client1's perspective
    cm1 = result1.get("causal_metadata", {})

    # Client 1 gets x from node 1
    r = client1.get(nodes[1], "x", causal_metadata=cm1)
    print(r)
    assert r["status_code"] == 200, f"expected 200 for GET x, got {r['status_code']}"
    assert r["value"] in ["1", "2"], f"expected value '1' or '2', got {r['value']}"

    return True, "Concurrent PUTs to same key test passed"


def partitioned_three_clients_concurrent_x(conductor: ClusterConductor, fx: KvsFixture):
    """
    Test: 2 nodes, 3 clients, partitioned.
    - Client 1 puts x=1 on node 1, then y=2 on node 2.
    - Client 2 puts x=3 on node 2.
    - Client 3 gets x from node 2 (should get 3).
    - Client 3 gets x from node 1 (should get 1).
    """
    nodes = conductor.spawn_cluster(node_count=2, nodes_per_shard=2)
    client1 = fx.create_client(name="client1")
    client2 = fx.create_client(name="client2")
    client3 = fx.create_client(name="client3")
    client1.broadcast_view(nodes)
    client2.broadcast_view(nodes)
    client3.broadcast_view(nodes)
    nodes = nodes["shard0"]

    # Partition the nodes so they cannot communicate
    conductor.create_partition([nodes[0]], "p0")
    conductor.create_partition([nodes[1]], "p1")

    log("\n> TEST: Partitioned Three Clients Concurrent X")

    cm1 = {}
    cm2 = {}
    cm3 = {}

    # Client 1 puts x=1 on node 1
    r1 = client1.put(nodes[0], "x", "1", causal_metadata=cm1)
    assert r1["status_code"] == 200, f"expected 200 for client1 PUT x on node1, got {r1['status_code']}"
    cm1 = r1["causal_metadata"]

    # Client 1 puts y=2 on node 2
    r2 = client1.put(nodes[1], "y", "2", causal_metadata=cm1)
    assert r2["status_code"] == 200, f"expected 200 for client1 PUT y on node2, got {r2['status_code']}"
    cm1 = r2["causal_metadata"]

    # Client 2 puts x=3 on node 2
    r3 = client2.put(nodes[1], "x", "3", causal_metadata=cm2)
    assert r3["status_code"] == 200, f"expected 200 for client2 PUT x on node2, got {r3['status_code']}"
    cm2 = r3["causal_metadata"]

    # Client 3 gets x from node 2 (should get 3)
    r4 = client3.get(nodes[1], "x", causal_metadata=cm3)
    assert r4["status_code"] == 200, f"expected 200 for client3 GET x on node2, got {r4['status_code']}"
    assert r4["value"] == "3", f"expected value '3' for x on node2, got {r4['value']}"
    cm3 = r4["causal_metadata"]

    # Client 3 gets x from node 1 (should get 1)
    r5 = client3.get(nodes[0], "x", causal_metadata=cm3)
    assert r5["status_code"] == 200, f"expected 200 for client3 GET x on node1, got {r5['status_code']}"
    assert r5["value"] == "1", f"expected value '1' for x on node1, got {r5['value']}"

    return True, "Partitioned three clients concurrent x test passed"


def partitioned_get_hangs_on_missing_dependency(conductor: ClusterConductor, fx: KvsFixture):
    """
    Test: 2 nodes, 2 clients, partitioned.
    - Client 1 puts x=1 on node 1, then y=2 on node 2.
    - Client 2 gets y from node 2 (should get 2).
    - Client 2 gets x from node 2 (should hang/timeout).
    """
    nodes = conductor.spawn_cluster(node_count=2, nodes_per_shard=2)
    client1 = fx.create_client(name="client1")
    client2 = fx.create_client(name="client2")
    client1.broadcast_view(nodes)
    client2.broadcast_view(nodes)
    nodes = nodes["shard0"]

    # Partition the nodes so they cannot communicate
    conductor.create_partition([nodes[0]], "p0")
    conductor.create_partition([nodes[1]], "p1")

    log("\n> TEST: Partitioned GET hangs on missing dependency")

    cm1 = {}
    cm2 = {}

    # Client 1 puts x=1 on node 1
    r1 = client1.put(nodes[0], "x", "1", causal_metadata=cm1)
    assert r1["status_code"] == 200, f"expected 200 for client1 PUT x on node1, got {r1['status_code']}"
    cm1 = r1["causal_metadata"]

    # Client 1 puts y=2 on node 2
    r2 = client1.put(nodes[1], "y", "2", causal_metadata=cm1)
    assert r2["status_code"] == 200, f"expected 200 for client1 PUT y on node2, got {r2['status_code']}"
    cm1 = r2["causal_metadata"]

    # Client 2 gets y from node 2 (should get 2)
    r3 = client2.get(nodes[1], "y", causal_metadata=cm2)
    assert r3["status_code"] == 200, f"expected 200 for client2 GET y on node2, got {r3['status_code']}"
    assert r3["value"] == "2", f"expected value '2' for y on node2, got {r3['value']}"
    cm2 = r3["causal_metadata"]

    # Client 2 gets x from node 2 (should hang/timeout, expect 408)
    r4 = client2.get(nodes[1], "x", causal_metadata=cm2)
    assert r4["status_code"] == 408, f"expected timeout (408) for client2 GET x on node2, got {r4['status_code']}"

    return True, "Partitioned get hangs on missing dependency test passed"


def test_partitioned_get_all(conductor: ClusterConductor, fx: KvsFixture):
    """
    - Client 1 puts x=1 on node 1
    - Client 1 puts y=2 on node 1
    - Partition node1 and node2
    - Client 1 performs GET /data on node2 (should return empty store)
    """
    nodes = conductor.spawn_cluster(node_count=2, nodes_per_shard=2)
    client1 = fx.create_client("client1")

    # Share view
    client1.broadcast_view(nodes)
    nodes = nodes["shard0"]

    # Partition the nodes
    conductor.create_partition([nodes[0]], "p0")
    conductor.create_partition([nodes[1]], "p1")

    log("\n> TEST: Partitioned GET ALL")

    cm = {}

    # PUT x=1 on node 1
    r1 = client1.put(nodes[0], "x", "1", causal_metadata=cm)
    assert r1["status_code"] == 200, f"PUT x on node1 failed: {r1['status_code']}"
    cm = r1["causal_metadata"]

    # PUT y=2 on node 1
    r2 = client1.put(nodes[0], "y", "2", causal_metadata=cm)
    assert r2["status_code"] == 200, f"PUT y on node1 failed: {r2['status_code']}"
    cm = r2["causal_metadata"]

    # GET ALL from node 2 timeout
    r3 = client1.get_all(nodes[1], causal_metadata=cm)
    assert r3["status_code"] == 408, f"GET /data on node2 shouldtimeout with: {r3['status_code']}"

    return True, "Partitioned GET ALL test passed"


def test_partitioned_puts_and_get_all(conductor: ClusterConductor, fx: KvsFixture):
    """
    Test scenario:
    - Partition nodes N1 and N2.
    - C1 puts x=a on N1.
    - C1 puts z=b on N2.
    - C2 puts x=c on N2.
    - C2 puts y=d on N2.
    - C2 does get_all on N2 and expects {z: b, x: c, y: d}.
    """
    nodes = conductor.spawn_cluster(node_count=2, nodes_per_shard=2)
    client1 = fx.create_client(name="client1")
    client2 = fx.create_client(name="client2")

    client1.broadcast_view(nodes)
    client2.broadcast_view(nodes)
    nodes = nodes["shard0"]

    # Partition the nodes so they cannot communicate
    conductor.create_partition([nodes[0]], "p0")
    conductor.create_partition([nodes[1]], "p1")

    log("\n> TEST: Partitioned puts and get_all on node2")

    cm1 = {}
    cm2 = {}

    # Client 1 puts x=a on node 1
    r1 = client1.put(nodes[0], "x", "a", causal_metadata=cm1)
    assert r1["status_code"] == 200, f"Expected 200 for client1 PUT x on node1, got {r1['status_code']}"
    cm1 = r1["causal_metadata"]

    # Client 1 puts z=b on node 2
    r2 = client1.put(nodes[1], "z", "b", causal_metadata=cm1)
    assert r2["status_code"] == 200, f"Expected 200 for client1 PUT z on node2, got {r2['status_code']}"
    cm1 = r2["causal_metadata"]

    # Client 2 puts x=c on node 2
    r3 = client2.put(nodes[1], "x", "c", causal_metadata=cm2)
    assert r3["status_code"] == 200, f"Expected 200 for client2 PUT x on node2, got {r3['status_code']}"
    cm2 = r3["causal_metadata"]

    # Client 2 puts y=d on node 2
    r4 = client2.put(nodes[1], "y", "d", causal_metadata=cm2)
    assert r4["status_code"] == 200, f"Expected 200 for client2 PUT y on node2, got {r4['status_code']}"
    cm2 = r4["causal_metadata"]

    # Client 2 does get_all on node 2
    r5 = client2.get_all(nodes[1], causal_metadata=cm2)
    assert r5["status_code"] == 200, f"Expected 200 for client2 GET_ALL on node2, got {r5['status_code']}"
    items = r5.get("items", {})

    expected_items = {"z": "b", "x": "c", "y": "d"}

    assert all(k in items and items[k] == v for k, v in expected_items.items()), (
        f"Expected items {expected_items}, got {items}"
    )

    return True, "Partitioned puts and get_all test passed"


def test_shard_merge_on_view_change(conductor: ClusterConductor, fx: KvsFixture):
    """
    - Create a cluster with 3 shards (e.g., 3 nodes per shard).
    - Put unique keys on each shard.
    - Check each shard's data is disjoint.
    - Remove 2 shards from the view.
    - Broadcast new view (only 1 shard).
    - Check get_all on the remaining shard returns all keys/values.
    """
    num_shards = 3
    nodes_per_shard = 3
    keys_per_shard = 5

    # 1. Spawn cluster and create clients
    nodes = conductor.alternative_spawn_cluster([[nodes_per_shard]] * num_shards)
    client = fx.create_client(name="client_merge")
    client.broadcast_view(nodes)

    # 2. Put unique keys on each shard
    all_keys = {}
    for shard_name, shard_nodes in nodes.items():
        shard_keys = []
        for i in range(keys_per_shard):
            key = f"{shard_name}_key_{i}"
            value = f"{shard_name}_val_{i}"
            r = client.put(shard_nodes[0], key, value, causal_metadata={})
            assert r["status_code"] == 200, f"PUT failed for {key} on {shard_name}"
            shard_keys.append((key, value))
        all_keys[shard_name] = shard_keys

    # 3. Check each shard's data is disjoint
    keysets = {}
    for shard_name, shard_nodes in nodes.items():
        r = client.get_all(shard_nodes[0], causal_metadata={})
        assert r["status_code"] == 200, f"GET_ALL failed on {shard_name}"
        items = r.get("items", {})
        keysets[shard_name] = set(items.keys())
    for i, s1 in enumerate(nodes.keys()):
        for s2 in list(nodes.keys())[i + 1 :]:
            assert keysets[s1].isdisjoint(keysets[s2]), f"Shards {s1} and {s2} have overlapping keys!"

    # 4. Remove 2 shards from the view
    remaining_shard = list(nodes.keys())[0]
    new_view = {remaining_shard: nodes[remaining_shard]}

    # 5. Broadcast new view (only 1 shard)
    client.broadcast_view(new_view)

    # 6. Check get_all on the remaining shard returns all keys/values
    r = client.get_all(nodes[remaining_shard][0], causal_metadata={})
    assert r["status_code"] == 200, f"GET_ALL failed on merged shard"
    items = r.get("items", {})

    # All keys/values from all shards should be present
    expected = {k: v for shard_keys in all_keys.values() for k, v in shard_keys}
    assert all(k in items and items[k] == v for k, v in expected.items()), (
        f"After merge, expected items {expected}, got {items}"
    )

    return True, "Shard merge on view change test passed"


BASIC_TESTS = [
    TestCase("basic_put_get_with_metadata", basic_put_get_with_metadata),
    TestCase("basic_causal_chain", basic_causal_chain),
    TestCase("test_shard_disjoint_get_all", test_shard_disjoint_get_all),
    TestCase("get_all_basic", get_all_basic),
    TestCase("basic_availability", basic_availability),
    TestCase("initial_view_empty", initial_view_empty),
    TestCase("inconsistent_causal_history_1", inconsistent_causal_history_1),
    TestCase("inconsistent_causal_history_2", inconsistent_causal_history_2),
    TestCase("partitioned_three_clients_visibility", partitioned_three_clients_visibility),
    TestCase("partitioned_get_hangs_on_missing_dependency", partitioned_get_hangs_on_missing_dependency),
    TestCase("partitioned_three_clients_concurrent_x", partitioned_three_clients_concurrent_x),
    TestCase("multiple_clients_causal_metadata", multiple_clients_causal_metadata),
    TestCase("partitioned_puts_and_gets", partitioned_puts_and_gets),
    TestCase("partitioned_sequential_puts_and_gets", partitioned_sequential_puts_and_gets),
    TestCase("concurrent_puts_same_key", concurrent_puts_same_key),
    TestCase("test_partitioned_get_all", test_partitioned_get_all),
    TestCase("test_partitioned_puts_and_get_all", test_partitioned_puts_and_get_all),
    TestCase("test_shard_merge_on_view_change", test_shard_merge_on_view_change),
]
