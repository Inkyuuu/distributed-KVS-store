import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import List

import docker
import docker.errors
import docker.types
import requests

from .util import log


class ContainerBuilder:
    def __init__(self, docker_client: docker.DockerClient, project_dir: str, image_id: str):
        self.project_dir = project_dir
        self.image_id = image_id
        self.client = docker_client

    def build_image(self) -> None:
        # ensure we are able to build the container image
        log(f"building container image {self.image_id}...")
        build_result = self.client.api.build(path=self.project_dir, rm=True, nocache=True, tag=self.image_id)

        # Consume the generator to ensure the build completes
        for chunk in build_result:
            if isinstance(chunk, dict) and "error" in chunk:
                raise Exception(f"Build error: {chunk['error']}")

        # ensure the image exists
        log(f"inspecting container image {self.image_id}...")
        self.client.api.inspect_image(self.image_id)

    def cleanup(self) -> None:
        # clean up the container image and any dangling images
        log(f"cleaning up container image {self.image_id}...")
        self.client.api.remove_image(image=self.image_id)
        self.client.api.prune_images(filters={})


@dataclass
class ClusterNode:
    name: str  # container name
    index: int  # container global id/index
    ip: str  # container ip on current/primary network
    port: int  # container http service port
    external_port: int  # host's mapped external port forwarded to container's service port
    networks: List[str]  # networks the container is attached to

    def internal_endpoint(self) -> str:
        return f"http://{self.ip}:{self.port}"

    def external_endpoint(self) -> str:
        return f"http://localhost:{self.external_port}"


@dataclass
class NetworkHandle:
    name: str


class ClusterConductor:
    def __init__(
        self,
        docker_client: docker.DockerClient,
        group_id: str,
        base_image: str,
        external_port_base: int = 8081,
    ):
        self.group_id = group_id
        self.base_image = base_image
        self.base_port = external_port_base
        self.nodes: List[ClusterNode] = []
        self.client = docker_client

        # naming patterns
        self.group_ctr_prefix = f"kvs_{group_id}_node"
        self.group_net_prefix = f"kvs_{group_id}_net"

        # base network
        self.base_net_name = f"{self.group_net_prefix}_base"

        # network subnet mappings
        self.network_subnets = {}

        self.wait_online_timeout_s = 10

    @property
    def base_net(self) -> NetworkHandle:
        return NetworkHandle(name=self.base_net_name)

    def _list_containers(self) -> List[str]:
        # get list of all container names
        return [c["Names"][0].removeprefix("/") for c in self.client.api.containers(all=True)]

    def _list_networks(self) -> List[str]:
        # get list of all network names
        return [n["Name"] for n in self.client.api.networks()]

    def _remove_container(self, name: str) -> None:
        # remove a single container
        log(f"removing container {name}")
        self.client.api.remove_container(container=name, force=True)

    def dump_logs(self, path: Path) -> None:
        """Dump container logs to files"""
        path.mkdir(parents=True, exist_ok=True)

        for node in self.nodes:
            log_file = path / f"{node.name}"
            with log_file.open("wb") as f:
                logs = self.client.api.logs(container=node.name, stdout=True, stderr=True)
                f.write(logs)
                log(f"dumped logs to {log_file}")

    def _remove_network(self, name: str) -> None:
        # remove a single network
        log(f"removing network {name}")
        self.client.networks.get(name).remove()

    def _list_subnets(self, net_name: str) -> list[str]:
        config = self.client.networks.get(net_name).attrs["IPAM"]["Config"]
        if config is None:
            return []
        return [c["Subnet"] for c in config]

    def create_network(self, name: str) -> NetworkHandle:
        log(f"creating network {name}")

        # check if network already exists
        exists = True
        try:
            self.client.networks.get(name)
        except docker.errors.NotFound:
            exists = False

        if exists:
            # network exists, get its subnet
            self.network_subnets[name] = self._list_subnets(net_name=name)[0]
            log(f"network {name} already exists with subnet {self.network_subnets[name]}")
            return NetworkHandle(name=name)

        # get subnets that are definitely in use
        try:
            used_subnets = {s for n in self._list_networks() for s in self._list_subnets(n)}
        except docker.errors.APIError as e:
            log(f"warning: error getting network info: {str(e)}")
            raise

        # try creating network with different subnets
        max_retries = 10
        for attempt in range(max_retries):
            # try different subnets (randomize to avoid conflicts between multiple processes)
            second_octet = 16 + (attempt % 16)
            third_octet = (attempt * 7 + hash(name)) % 256  # deterministic but varies by name

            subnet = f"172.{second_octet}.{third_octet}.0/24"
            if subnet in used_subnets:
                continue

            try:
                self.client.networks.create(
                    name=name,
                    ipam=docker.types.IPAMConfig(pool_configs=[docker.types.IPAMPool(subnet=subnet)]),
                )
            except docker.errors.APIError as e:
                if "Pool overlaps" in str(e):
                    # this subnet is in use but wasn't detected earlier
                    used_subnets.add(subnet)
                    log(f"subnet {subnet} is in use, trying another one")
                else:
                    log(f"error creating network: {e}")
                continue

            log(f"created network {name} with subnet {subnet}")
            return NetworkHandle(name=name)

        # if we get here, we've exhausted all retries
        raise RuntimeError(f"failed to create network {name} after {max_retries} attempts")

    def _network_exists(self, name: str) -> bool:
        return name in self._list_networks()

    def cleanup_hanging(self, group_only: bool = True) -> None:
        # if group_only, only clean up stuff for this group
        # otherwise clean up anything kvs related
        if group_only:
            log(f"cleaning up group {self.group_id}")
            container_pattern = f"^kvs_{self.group_id}_.*"
            network_pattern = f"^kvs_{self.group_id}_net_.*"
        else:
            log("cleaning up all kvs containers and networks")
            container_pattern = "^kvs_.*"
            network_pattern = "^kvs_net_.*"

        # compile regex patterns
        container_regex = re.compile(container_pattern)
        network_regex = re.compile(network_pattern)

        # cleanup containers
        log(f"  cleaning up {'group' if group_only else 'all'} containers")
        containers = self._list_containers()
        for container in containers:
            if container and container_regex.match(container):
                self._remove_container(container)

        # cleanup networks
        log(f"  cleaning up {'group' if group_only else 'all'} networks")
        networks = self._list_networks()
        for network in networks:
            if network and network_regex.match(network):
                self._remove_network(network)

    # we can check if a node is online by GET /ping
    def _is_online(self, node: ClusterNode) -> bool:
        try:
            r = requests.get(f"{node.external_endpoint()}/ping", timeout=10)
            return r.status_code == 200
        except requests.exceptions.RequestException as e:
            log(f"node {node.name} is not online: {e}")
            return False

    def _node_name(self, index: int) -> str:
        return f"kvs_{self.group_id}_node_{index}"

    def _get_container_ip(self, container_name: str, network_name: str) -> str:
        try:
            attrs = self.client.containers.get(container_name).attrs
        except docker.errors.APIError as e:
            log(f"failed to inspect container {container_name}")
            log(e)
            raise
        container_ip = attrs["NetworkSettings"]["Networks"][network_name]["IPAddress"]
        return container_ip

    # create a cluster of nodes on the base network
    def spawn_cluster(self, node_count: int, nodes_per_shard: int) -> list[ClusterNode]:
        log(f"spawning cluster of {node_count} nodes with up to {nodes_per_shard} nodes per shard")

        spawned = [self.spawn_node(network=self.base_net) for _ in range(node_count)]

        shards = {}
        for i, node in enumerate(spawned):
            shard_key = f"shard{i // nodes_per_shard}"
            if shard_key not in shards:
                shards[shard_key] = []
            shards[shard_key].append(node)
        # wait for the nodes to come online (sequentially)
        log("waiting for nodes to come online...")
        wait_online_start = time.time()
        for node in spawned:
            while not self._is_online(node):
                if time.time() - wait_online_start > self.wait_online_timeout_s:
                    raise RuntimeError(f"node {node.name} did not come online")
                time.sleep(0.2)

            log(f"  node {node.name} online")

        log("all nodes online")

        return shards

    def alternative_spawn_cluster(self, shard_node_counts: list[list[int]]) -> list[ClusterNode]:
        total_nodes = sum(count[0] for count in shard_node_counts)

        log(f"spawning cluster with {len(shard_node_counts)} shards and {total_nodes} nodes")

        spawned = [self.spawn_node(network=self.base_net) for _ in range(total_nodes)]

        shards = {}
        node_idx = 0
        for shard_num, count_list in enumerate(shard_node_counts):
            count = count_list[0]
            shard_key = f"shard{shard_num}"
            shards[shard_key] = []
            for _ in range(count):
                shards[shard_key].append(spawned[node_idx])
                node_idx += 1

        # Wait for the nodes to come online (sequentially)
        log("waiting for nodes to come online...")
        wait_online_start = time.time()
        for node in spawned:
            while not self._is_online(node):
                if time.time() - wait_online_start > self.wait_online_timeout_s:
                    raise RuntimeError(f"node {node.name} did not come online")
                time.sleep(0.2)
            log(f"  node {node.name} online")

        log("all nodes online")
        return shards

    def spawn_node(self, network: NetworkHandle) -> ClusterNode:
        # spawn the nodes
        node_idx = len(self.nodes)
        node_name = self._node_name(node_idx)
        # map to sequential external port
        external_port = self.base_port + node_idx
        port = 8081  # internal port

        log(f"  starting container {node_name} (ext_port={external_port})")

        # start container detached from networks
        self.client.containers.run(
            image=self.base_image,
            detach=True,
            name=node_name,
            remove=True,
            environment={"NODE_IDENTIFIER": f"{node_idx}"},
            ports={f"{port}": external_port},
        )

        # attach container to base network
        log(f"    attaching container {node_name} to base network")
        self.client.networks.get(network.name).connect(node_name)

        # inspect the container to get ip, etc.
        log(f"    inspecting container {node_name}")
        container_ip = self._get_container_ip(node_name, self.base_net_name)

        # store container metadata
        node = ClusterNode(
            name=node_name,
            index=node_idx,
            ip=container_ip,
            port=port,
            external_port=external_port,
            networks=[self.base_net_name],
        )
        self.nodes.append(node)

        log(f"    container {node_name} spawned, base_net_ip={container_ip}")

        return node

    def destroy_cluster(self) -> None:
        # clean up after this group
        self.cleanup_hanging(group_only=True)

        # clear nodes
        self.nodes.clear()

    def describe_cluster(self) -> None:
        log(f"TOPOLOGY: group {self.group_id}")
        log("nodes:")
        for node in self.nodes:
            log(f"  {node.name}: {node.ip}:{node.port} <-> localhost:{node.external_port}")

        # now log the partitions and the nodes they contain
        partitions = {}
        for node in self.nodes:
            for network in node.networks:
                if network not in partitions:
                    partitions[network] = []
                partitions[network].append(node.index)

        log("partitions:")
        for net, nodes in partitions.items():
            part_name = net[len(self.group_net_prefix) + 1 :]
            log(f"  {part_name}: {nodes}")

    def create_partition(self, nodes: list[ClusterNode], partition_id: str) -> NetworkHandle:
        net_name = f"kvs_{self.group_id}_net_{partition_id}"

        log(f"creating partition {partition_id} with nodes {[n.index for n in nodes]}")

        # create partition network if it doesn't exist
        if not self._network_exists(net_name):
            self.create_network(net_name)

        # disconnect specified nodes from all other networks
        log("  disconnecting nodes from other networks")
        for node in nodes:
            for network in node.networks:
                if network != net_name:
                    log(f"    disconnecting {node.name} from network {network}")
                    self.client.networks.get(network).disconnect(node.name)
                    node.networks.remove(network)

        # connect nodes to partition network, and update node ip
        log(f"  connecting nodes to partition network {net_name}")
        for node in nodes:
            log(f"    connecting {node.name} to network {net_name}")
            self.client.networks.get(net_name).connect(node.name)
            node.networks.append(net_name)

            # update node ip on the new network
            container_ip = self._get_container_ip(node.name, net_name)
            log(f"    node {node.name} ip in network {net_name}: {container_ip}")

            # update node ip
            node.ip = container_ip

        return NetworkHandle(name=net_name)

    def simulate_kill_node(self, node: ClusterNode, network: NetworkHandle) -> None:
        # knock a node off the network it's on
        log(f"simulating kill of node {node.name} on network {network.name}")

        # disconnect node from network
        self.client.networks.get(network.name).disconnect(node.name)

        # remove network from node's list
        node.networks.remove(network.name)

    def simulate_revive_node(self, node: ClusterNode, network: NetworkHandle) -> None:
        # revive a node on the network it's supposed to be on
        log(f"simulating revive of node {node.name} on network {network.name}")

        # connect node to network
        # try to preserve the same ip
        self.client.api.connect_container_to_network(net_id=network.name, container=node.name, ipv4_address=node.ip)

        # add network to node's list
        node.networks.append(network.name)

    def __enter__(self):
        self.create_network(self.base_net_name)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # clean up automatically
        self.destroy_cluster()
