from multiprocessing.pool import ThreadPool

from ..containers import ClusterConductor
from ..hw2_api import KvsFixture
from ..testcase import TestCase
from ..util import log


def basic_kvs_put_get_1(conductor: ClusterConductor, fx: KvsFixture):
    a, b = conductor.spawn_cluster(node_count=2)
    mc = fx.create_client(name="tester")
    mc.broadcast_view([a, b])

    # let's see if it behaves like a kvs
    log("\n> TEST KVS PUT/GET")

    # put a new kvp
    r = mc.put(a, "test1", "hello")
    assert r.status_code == 201, f"expected 201 for new key, got {r.status_code}"

    # get should return same value
    r = mc.get(a, "test1")
    assert r.status_code == 200, f"expected 200 for get, got {r.status_code}"
    assert r.value == "hello", f"wrong value returned: {r.value}"

    # we should be able to update it
    r = mc.put(a, "test1", "world")
    assert r.status_code == 200, f"expected 200 for update, got {r.status_code}"

    # verify update worked
    r = mc.get(a, "test1")
    assert r.value == "world", "update failed"

    # return score/reason
    return True, "ok"


def basic_kvs_put_get_2(conductor: ClusterConductor, fx: KvsFixture):
    a, b = conductor.spawn_cluster(node_count=2)
    mc = fx.create_client(name="tester")
    mc.broadcast_view([a, b])

    # let's see if it behaves like a kvs
    log("\n> TEST KVS PUT/GET")

    # put a new kvp
    r = mc.put(a, "test1", "hello")
    assert r.status_code == 201, f"expected 201 for new key, got {r.status_code}"

    # now let's talk to the other node and see if it agrees
    r = mc.get(b, "test1")
    assert r.value == "hello", "node 1 did not agree"

    # now let's try updating via node 1 and see if node 0 agrees
    r = mc.put(b, "test1", "bye")
    assert r.status_code == 200, f"expected 200 for update, got {r.status_code}"

    # see if node 0 agrees
    r = mc.get(a, "test1")
    assert r.value == "bye", "node 0 did not agree"

    # return score/reason
    return True, "ok"


def basic_kvs_shrink_1(conductor: ClusterConductor, fx: KvsFixture):
    [a, b, c] = conductor.spawn_cluster(node_count=3)
    mc = fx.create_client(name="tester")
    mc.broadcast_view([a, b, c])

    # put a new kvp
    log("\n> PUT NEW KEY")
    r = mc.put(a, "test1", "hello")
    assert r.status_code == 201, f"expected 201 for new key, got {r.status_code}"

    # now let's isolate each node and make sure they agree
    log("\n> ISOLATE NODES")

    # isolate node 0
    conductor.create_partition([a], "p0")
    mc.broadcast_view([a])

    # isolate node 1
    conductor.create_partition([b], "p1")
    mc.broadcast_view([b])

    # isolate node 2
    conductor.create_partition([c], "p2")
    mc.broadcast_view([c])

    # describe the new network topology
    log("\n> NETWORK TOPOLOGY")
    conductor.describe_cluster()

    # make sure the data is there
    log("\n> MAKE SURE EACH ISOLATED NODE HAS THE DATA")

    r = mc.get(a, "test1")
    assert r.value == "hello", "node 0 lost data"

    r = mc.get(b, "test1")
    assert r.value == "hello", "node 1 lost data"

    r = mc.get(c, "test1")
    assert r.value == "hello", "node 2 lost data"

    # return score/reason
    return True, "ok"


def basic_kvs_split_1(conductor: ClusterConductor, fx: KvsFixture):
    [a, b, c, d] = conductor.spawn_cluster(node_count=4)
    mc = fx.create_client(name="tester")
    mc.broadcast_view([a, b, c, d])

    # put a new kvp
    log("\n> PUT NEW KEY")
    r = mc.put(a, "test1", "hello")
    assert r.status_code == 201, f"expected 201 for new key, got {r.status_code}"

    # now let's partition 0,1 and 2,3
    log("\n> PARTITION NODES")

    # partition 0,1
    conductor.create_partition([a, b], "p0")
    mc.broadcast_view([a, b])

    # partition 2,3
    conductor.create_partition([c, d], "p1")
    mc.broadcast_view([c, d])

    # describe the new network topology
    log("\n> NETWORK TOPOLOGY")
    conductor.describe_cluster()

    # send different stuff to each partition
    log("\n> TALK TO PARTITION p0")
    r = mc.get(a, "test1")
    assert r.value == "hello", "node 0 lost data"

    r = mc.put(b, "test2", "01")
    assert r.status_code == 201, f"expected 201 for new key, got {r.status_code}"

    r = mc.get(a, "test2")
    assert r.value == "01", "node 0 disagreed"

    log("\n> TALK TO PARTITION p1")
    r = mc.put(c, "test3", "23")
    assert r.status_code == 201, f"expected 201 for new key, got {r.status_code}"

    r = mc.get(d, "test3")
    assert r.value == "23", "node 3 disagreed"

    # return score/reason
    return True, "ok"


def kvs_put_delete(conductor: ClusterConductor, fx: KvsFixture):
    nodes = conductor.spawn_cluster(node_count=4)
    mc = fx.create_client(name="tester")
    mc.broadcast_view(nodes)
    testkeys = ["test1", "test2", "test3", "test4"]
    values = [1, 2, 3, 4]
    log("\n> PUT NEW KEY")
    for node, key, value in zip(nodes, testkeys, values):
        r = mc.put(node, key, str(value))
        assert r.status_code == 201, f"expected 201 for new key, got {r.status_code}"

    log("\n>DELETE FROM BACKUP")
    r = mc.delete(nodes[3], "test1")
    assert r.status_code == 200, f"expected 200 for ok, got {r.status_code}"

    conductor.create_partition(nodes[:2], "p0")
    conductor.create_partition(nodes[2:], "p1")
    mc.broadcast_view(nodes[:2])
    mc.broadcast_view(nodes[2:])

    for key in testkeys[1:]:
        r = mc.delete(nodes[1], key)
        assert r.status_code == 200, f"p0: expected 200 for ok, got {r.status_code}"
        r = mc.delete(nodes[3], key)
        assert r.status_code == 200, f"p1: expected 200 for ok, got {r.status_code}"
    r = mc.get_all(nodes[1])
    assert r.status_code == 200, f"p0: get_all expected 200 for ok, got {r.status_code}"
    assert len(r.values) == 0, "p0: expected empty dictionary"
    r = mc.get_all(nodes[3])
    assert r.status_code == 200, f"p1: get_all expected 200 for ok, got {r.status_code}"
    assert len(r.values) == 0, "p1: expected empty dictionary"

    return True, "ok"


def kvs_kill_node_1(conductor: ClusterConductor, fx: KvsFixture):
    nodes = conductor.spawn_cluster(node_count=4)
    view_broadcast_client = fx.create_client(name="view_broadcast")
    view_broadcast_client.broadcast_view(nodes)

    client = fx.create_client(name="tester")

    # Ensure nodes are communicating
    log("> add new keys to verify connection")
    for i, node in enumerate(nodes):
        r = client.put(node, f"test-key-{i}", f"test-value-{i}")
        assert r.status_code == 201, f"expected 201 for new key, got {r.status_code}"

    # Kill one node
    conductor.simulate_kill_node(nodes[3], conductor.base_net)
    log("> put while node is dead (should timeout)")
    with ThreadPool() as pool:
        results = pool.map(lambda it: client.put(it[1], f"key-{it[0]}", f"value-{it[0]}"), enumerate(nodes[:3]))
    for r in results:
        assert r.status_code == 408, f"expected timeout, got {r.status_code}"

    # Reinstate the node
    conductor.simulate_revive_node(nodes[3], conductor.base_net)
    log("> put while node is alive")
    for i, node in enumerate(nodes):
        r = client.put(node, f"key-{i}", f"value-{i}")
        assert r.ok, f"expected ok for new key, got {r.status_code}"

    return True, "ok"


def higher_node_count_puts(conductor: ClusterConductor, fx: KvsFixture):
    a, b, c, d, e, f, g = conductor.spawn_cluster(node_count=7)
    mc = fx.create_client(name="tester")
    mc.broadcast_view([a, b, c, d, e, f, g])

    # let's see if it behaves like a kvs
    log("\n> TEST KVS PUT/GET")

    # put a new kvp
    r = mc.put(d, "test1", "hello")
    assert r.status_code == 201, f"expected 201 for new key, got {r.status_code}"

    # get should return same value
    r = mc.get(c, "test1")
    assert r.status_code == 200, f"expected 200 for get, got {r.status_code}"
    assert r.value == "hello", f"wrong value returned: {r.value}"

    # we should be able to update it
    r = mc.put(c, "test1", "world")
    assert r.status_code == 200, f"expected 200 for update, got {r.status_code}"

    # verify update worked
    r = mc.get(a, "test1")
    assert r.value == "world", "update failed"

    # return score/reason
    return True, "ok"


def test_strong_consistency(conductor: ClusterConductor, fx: KvsFixture):
    nodes = conductor.spawn_cluster(node_count=3)
    mc = fx.create_client(name="tester")
    mc.broadcast_view(nodes)

    # Write a key-value pair to one node
    log("\n> WRITE TO NODE 0")
    r = mc.put(nodes[0], "key1", "value1")
    assert r.status_code == 201, f"expected 201 for new key, got {r.status_code}"

    # Ensure all nodes return the same value
    log("\n> VERIFY CONSISTENCY ACROSS NODES")
    for node in nodes:
        r = mc.get(node, "key1")
        assert r.status_code == 200, f"expected 200 for get, got {r.status_code}"
        assert r.value == "value1", f"expected value1, got {r.value}"

    return True, "Strong consistency verified"


def test_durability_on_isolation(conductor: ClusterConductor, fx: KvsFixture):
    nodes = conductor.spawn_cluster(node_count=3)
    mc = fx.create_client(name="tester")
    mc.broadcast_view(nodes)
    log("\n> WRITE TO NODE 0")
    r = mc.put(nodes[0], "key2", "value2")
    assert r.status_code == 201, f"expected 201 for new key, got {r.status_code}"

    log("\n> ISOLATE NODE 0")
    conductor.create_partition([nodes[1], nodes[2]], "p1")
    mc.broadcast_view([nodes[1], nodes[2]])
    log("\n> VERIFY DURABILITY ON NODE 1 AND NODE 2")
    for node in nodes[1:]:
        r = mc.get(node, "key2")
        assert r.status_code == 200, f"expected 200 for get, got {r.status_code}"
        assert r.value == "value2", f"expected value2, got {r.value}"

    return True, "Durability verified after isolation"


def test_concurrent_writes(conductor: ClusterConductor, fx: KvsFixture):
    nodes = conductor.spawn_cluster(node_count=3)
    mc = fx.create_client(name="tester")
    mc.broadcast_view(nodes)

    # Perform concurrent writes
    log("\n> PERFORM CONCURRENT WRITES")
    for i in range(100):
        key = "drfdfd"  # Change the key for each iteration
        value1 = f"value-{i}-a"  # Change the first value for each iteration
        value2 = f"value-{i}-b"  # Change the second value for each iteration
        value3 = f"value-{i}-c"
        value4 = f"value-{i}-d"
        with ThreadPool() as pool:
            results = pool.map(
                lambda args: mc.put(args[0], key, args[1]),
                [(nodes[0], value1), (nodes[1], value2), (nodes[0], value3), (nodes[1], value4)],
            )

        # Ensure one value is consistently stored
        log(f"\n> VERIFY CONSISTENCY FOR {key}")
        final_value = None
        for node in nodes:
            # Partition the node being checked
            mc.broadcast_view([node])

            # Check the value on the isolated node
            r = mc.get(node, key)
            assert r.status_code == 200, f"expected 200 for get, got {r.status_code}"
            if final_value is None:
                final_value = r.value
            assert r.value == final_value, f"expected {final_value}, got {r.value}"

            # Restore the cluster view after checking
            mc.broadcast_view(nodes)

    return True, "Concurrent writes resolved consistently"


def test_primary_die(conductor: ClusterConductor, fx: KvsFixture):
    nodes = conductor.spawn_cluster(node_count=4)
    mc = fx.create_client(name="tester")
    mc.broadcast_view(nodes)

    # Perform concurrent writes while killing the primary node
    log("\n> PERFORM CONCURRENT WRITES AND KILL PRIMARY NODE")

    def kill_primary():
        # Simulate killing the primary node
        conductor.simulate_kill_node(nodes[0], conductor.base_net)

    def perform_puts():
        # Perform PUT requests to the remaining nodes
        with ThreadPool() as pool:
            results = pool.map(
                lambda it: mc.put(it[1], f"key-{it[0]}", f"value-{it[0]}"),
                enumerate(nodes[:3]),
            )
        return results

    # Run both actions concurrently
    with ThreadPool() as pool:
        results = pool.map(lambda func: func(), [kill_primary, perform_puts])

    # Verify that PUT requests timed out
    for r in results[1]:  # The second result contains the PUT responses
        assert r.status_code == 408, f"expected timeout, got {r.status_code}"

    return True, "Primary died during concurrent writes"


def test_kvs_size_after_killing_node(conductor: ClusterConductor, fx: KvsFixture):
    nodes = conductor.spawn_cluster(node_count=4)
    mc = fx.create_client(name="tester")
    mc.broadcast_view(nodes)

    # Perform 4 PUT operations to populate the KVS
    log("\n> PERFORMING PUT OPERATIONS")
    test_keys = ["key1", "key2", "key3", "key4"]
    test_values = ["value1", "value2", "value3", "value4"]

    for key, value in zip(test_keys, test_values):
        r = mc.put(nodes[0], key, value)
        assert r.status_code == 201, f"expected 201 for {key}, got {r.status_code}"

    # Kill the 3rd node
    log("\n> KILL NODE 3")
    conductor.simulate_kill_node(nodes[2], conductor.base_net)

    # Wait for the system to react and make sure we hang (timeout should occur here)
    log("\n> ATTEMPTING PUT AFTER NODE 3 IS KILLED (EXPECT HANG OR TIMEOUT)")
    with ThreadPool() as pool:
        results = pool.map(lambda args: mc.put(args[1], f"key-{args[0]}", f"value-{args[0]}"), enumerate(nodes[:2]))

    # Ensure we get timeouts because node 3 is down and partitioned
    for r in results:
        assert r.status_code == 408, f"expected timeout, got {r.status_code}"

    # Revive the 3rd node
    log("\n> REVIVE NODE 3")
    conductor.simulate_revive_node(nodes[2], conductor.base_net)

    # Partition node 3 into its own isolated cluster
    log("\n> PARTITION NODE 3 INTO ITS OWN CLUSTER")
    conductor.create_partition([nodes[2]], "p2")

    # Partition the remaining nodes into their own group
    log("\n> PARTITION NODES 1, 2, 4 INTO SEPARATE CLUSTER")
    conductor.create_partition([nodes[0], nodes[1], nodes[3]], "p1")

    # Broadcast view updates
    mc.broadcast_view([nodes[2]])  # Node 3 gets its own view
    mc.broadcast_view([nodes[0], nodes[1], nodes[3]])  # Remaining cluster updates their view

    # Verify KVS sizes on the first and third nodes (they should not match)
    log("\n> VERIFY KVS SIZE ON NODE 1")
    r = mc.get_all(nodes[0])
    assert r.status_code == 200, f"expected 200 for get_all, got {r.status_code}"
    node_1_kvs_size = len(r.values)

    log("\n> VERIFY KVS SIZE ON NODE 3")
    r = mc.get_all(nodes[2])
    assert r.status_code == 200, f"expected 200 for get_all, got {r.status_code}"
    node_3_kvs_size = len(r.values)

    # Assert that the KVS sizes on the two nodes do not match due to partitioning
    assert node_1_kvs_size == node_3_kvs_size, (
        f"Expected same KVS sizes, but got {node_1_kvs_size} and {node_3_kvs_size}"
    )

    return True, "KVS size verified correctly after killing a node and partitioning"


def partition_then_unpartition(conductor: ClusterConductor, fx: KvsFixture):
    # initializing
    nodes = conductor.spawn_cluster(node_count=4)
    mc = fx.create_client(name="tester")
    mc.broadcast_view(nodes)
    # create a partition between nodes and ensure consistency within groups, but not between them
    conductor.create_partition([nodes[0], nodes[1]], "p0")
    mc.broadcast_view([nodes[0], nodes[1]])

    r = mc.put(nodes[0], "key1", "value1")
    assert mc.get(nodes[0], "key1").value == mc.get(nodes[1], "key1").value, (
        "partitioned group 1 does not maintain consistency"
    )

    conductor.create_partition([nodes[2], nodes[3]], "p1")
    mc.broadcast_view([nodes[2], nodes[3]])

    r = mc.put(nodes[2], "key2", "value2")
    assert mc.get(nodes[2], "key2").value == mc.get(nodes[3], "key2").value, (
        "partitioned group 2 does not maintain consistency"
    )

    assert mc.get(nodes[0], "key1").value != mc.get(nodes[2], "key2").value, (
        "partitioned group 1 and partitioned group 2 have the same value"
    )
    # take partition away, nodes should now work as normal
    mc.broadcast_view(nodes)
    r = mc.put(nodes[0], "key3", "value3")
    assert (
        mc.get(nodes[0], "key3").value
        == mc.get(nodes[1], "key3").value
        == mc.get(nodes[2], "key3").value
        == mc.get(nodes[3], "key3").value
    ), "healed node pool doesn't match"

    return True, "partitioned groups behave as expected"


def test_concurrent_puts_and_deletes(conductor: ClusterConductor, fx: KvsFixture):
    nodes = conductor.spawn_cluster(node_count=3)
    mc = fx.create_client(name="tester")
    mc.broadcast_view(nodes)

    # Perform concurrent writes
    log("\n> PERFORM CONCURRENT WRITES")
    for i in range(100):
        key = "drfdfd"  # Change the key for each iteration
        value1 = f"value-{i}-a"
        value2 = f"value-{i}-b"
        operations = [
            (mc.put, nodes[0], key, value1),  # PUT on node 0
            (mc.put, nodes[1], key, value2),  # PUT on node 1
            (mc.delete, nodes[2], key),  # DELETE on node 2
        ]
        with ThreadPool() as pool:
            results = pool.map(
                lambda op: op[0](op[1], op[2], op[3]) if op[0] == mc.put else op[0](op[1], op[2]),
                operations,
            )
        for r in results:
            assert r.status_code in [200, 201, 404], f"Unexpected status code: {r.status_code}"

        # Ensure one value is consistently stored
        log(f"\n> VERIFY CONSISTENCY FOR {key}")
        final_value = None
        first_code = None
        for node in nodes:
            mc.broadcast_view([node])

            r = mc.get(node, key)
            if first_code is None:
                first_code = r.status_code
            assert r.status_code == first_code, f"expected matching code, got {first_code} and {r.status_code}"
            if r.status_code == 200:
                if final_value is None:
                    final_value = r.value
                assert r.value == final_value, f"expected {final_value}, got {r.value}"
            else:
                assert r.status_code == 404, f"expected 404 for missing key, got {r.status_code}"
            mc.broadcast_view(nodes)

    return True, "Concurrent writes and deletes resolved consistently"


BASIC_TESTS = [
    TestCase("basic_kvs_put_get_1", basic_kvs_put_get_1),
    TestCase("basic_kvs_put_get_2", basic_kvs_put_get_2),
    TestCase("basic_kvs_shrink_1", basic_kvs_shrink_1),
    TestCase("basic_kvs_split_1", basic_kvs_split_1),
    TestCase("kvs_put_delete", kvs_put_delete),
    TestCase("kvs_kill_node_1", kvs_kill_node_1),
    TestCase("higher_node_count_puts", higher_node_count_puts),
    TestCase("test_strong_consistency", test_strong_consistency),
    TestCase("test_durability_on_isolation", test_durability_on_isolation),
    TestCase("test_concurrent_writes", test_concurrent_writes),
    TestCase("test_primary_die", test_primary_die),
    TestCase("test_kvs_size_after_killing_node", test_kvs_size_after_killing_node),
    TestCase("test_concurrent_puts_and_deletes", test_concurrent_puts_and_deletes),
]
