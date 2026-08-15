#!/usr/bin/env python3

import argparse
import datetime
import os
from pathlib import Path

import docker

# from . import hw2_tests
from . import hw4_tests
from .containers import ClusterConductor, ContainerBuilder
from .hw4_api import KvsFixture
from .util import capture_logs, log

CONTAINER_IMAGE_ID = "kvstore-hw4-test"
TEST_GROUP_ID = "hw4"


class TestRunner:
    def __init__(self, project_dir: str, docker_client: docker.DockerClient):
        self.project_dir = project_dir
        # builder to build container image
        self.builder = ContainerBuilder(
            docker_client=docker_client,
            project_dir=project_dir,
            image_id=CONTAINER_IMAGE_ID,
        )
        # network manager to mess with container networking
        self.conductor = ClusterConductor(
            docker_client=docker_client,
            group_id=TEST_GROUP_ID,
            base_image=CONTAINER_IMAGE_ID,
            external_port_base=9000,
        )

    def prepare_environment(self) -> None:
        log("\n-- prepare_environment --")
        # build the container image
        if os.environ.get("KVS_SKIP_BUILD") == "1":
            log("Skipping image build (KVS_SKIP_BUILD=1)")
        else:
            self.builder.build_image()

        # aggressively clean up anything kvs-related
        # NOTE: this disallows parallel run processes, so turn it off for that
        self.conductor.cleanup_hanging(group_only=False)

    def cleanup_environment(self) -> None:
        log("\n-- cleanup_environment --")
        # destroy the cluster
        self.conductor.destroy_cluster()
        # aggressively clean up anything kvs-related
        # NOTE: this disallows parallel run processes, so turn it off for that
        self.conductor.cleanup_hanging(group_only=True)


"""
TEST SET: this list the test cases to run
add more tests by appending to this list
"""

TEST_SET = [
    *hw4_tests.basic.BASIC_TESTS,
    *hw4_tests.sharded.SHARD_TESTS,
]


def parse_args():
    parser = argparse.ArgumentParser(description="Run KVS cluster tests")
    parser.add_argument("path", help="Path to the project directory containing Dockerfile")
    parser.add_argument("-f", "--filter", help="Filter tests by name")
    parser.add_argument(
        "--no-fail-fast",
        action="store_false",
        dest="fail_fast",
        help="Continue testing even if a test case fails",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Directory to save test results and logs",
    )
    return parser.parse_args()


def main():
    # parse command line arguments
    args = parse_args()

    # use provided project directory path
    project_dir = args.path
    runner = TestRunner(project_dir=project_dir, docker_client=docker.from_env())

    # prepare to run tests
    runner.prepare_environment()

    tests = list(TEST_SET)

    if args.output_dir is None:
        args.output_dir = Path(args.path, "test_results")
    output_dir = args.output_dir / datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    # apply test filter if provided
    if args.filter:
        log(f"filtering tests by: {args.filter}")
        tests = [t for t in TEST_SET if args.filter in t.name]

    # run tests
    log("\n== RUNNING TESTS ==")
    run_tests = []
    for test in tests:
        test_dir = output_dir / test.name

        should_stop = False

        log("\n")
        with capture_logs() as logs:
            log(f"== TEST: [{test.name}] ==\n")
            run_tests.append(test)
            with runner.conductor:
                # record clients created to save logs later
                fx = KvsFixture()
                score, reason = test.execute(runner.conductor, fx)
                # dump container logs
                runner.conductor.dump_logs(path=test_dir / "nodes")
                # dump client logs
                for client in fx.clients:
                    client.dump_logs(path=test_dir / "clients")

            log("\n")
            if score:
                log(f"✓ PASSED {test.name}")
            else:
                log(f"✗ FAILED {test.name}: {reason}")
                if args.fail_fast:
                    log("FAIL FAST enabled, stopping at first failure")
                    should_stop = True

        # save test log
        (test_dir / "log.txt").write_text(logs.buffer, encoding="utf-8")

        if should_stop:
            break

    log("\n")

    with capture_logs() as logs:
        # summarize the status of all tests
        log("== TEST SUMMARY ==")
        for test in run_tests:
            log(f"  - {test.name}: {'✓' if test.score else '✗'}")

    # save summary
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "summary.txt").write_text(logs.buffer, encoding="utf-8")

    # clean up
    runner.cleanup_environment()


if __name__ == "__main__":
    main()
