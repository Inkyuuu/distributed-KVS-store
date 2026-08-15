# Using the Test Bench
This test bench is designed as a framework for testing all of the
distributed assignments (2, 3, and 4). It handles a lot of the underlying
infrastructure for many features you'll use repeatedly, including:

1. Building a docker image from your Dockerfile, and creating containers
from that image. (Containers are essentially instances of your image.)
2. Attaching docker containers to a docker network. This coalesces containers
and gives them new addresses within the network, and is useful for creating
partitions.
3. Sending http requests to nodes in the K/V Store.

This is primarily designed as a framework, and the tests that are provided
are only instructional examples. They do not comprehensively test the
correctness of your implementation, merely provide a demonstration of the
API for creating tests with this framework. It is up to you to test your
code. 

## Setup

You should create a venv (virtual environment) prior to running the tests.
If you haven't encountered a venv before, using one allows you to safely
install python packages without potentially breaking your system-wide
python installation, which your operating system may depend on (particularly
if you're using linux). To set up a venv and install the required packages,
use the commands below.  

The default `python3.*-venv` is perfectly good, but if you're
interested in a very fast python package manager written in
Rust, check out uv! https://docs.astral.sh/uv/


```sh
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## How to Run

Run all tests:

```sh
python -m kvs_test <project_dir>
```

Run tests filtered by name:

```sh
python -m kvs_test <project_dir> -f <filter>
```

## Adding Tests

- See the example tests under `hw*_tests/`.
- Write a function in one of those files, or add a new file based on one of those.
- Add your test to list of tests at the bottom of
that file.
- If you created a new file, update `TEST_SET` in `__main__.py`
to add your tests to the list.

### Test Parameters

Every test function takes a `ClusterConductor` and a `KvsFixture` parameter. You're encouraged to examine these for yourself, but
here are the basics:  

**ClusterConductor**:  
- Contains a list of nodes, and a bunch of information about them
such as ports and subnets (docker networks) that nodes are attached
to.
- Creates networks and partitions, gets views of nodes based on
partitions, and generally handles attaching nodes to/from networks.
- Dumps logs of each node on cleanup. This means that print
statements, errors, etc. from each node (docker container) will
end up in a log file. This is very helpful for debugging!

**KvsFixture**:  
- Allows you to create `KvsClient`s, and stores all clients created
  so that client logs can be saved after each test.

**KvsClient**:
- This has methods for sending HTTP requests in the right format to the KVS.
  This includes PUT/GET/DELETE operations, and view changes
- Creates logs of every request and response sent to and from your
  K/V store.


## Tips
- Design your tests with a Lamport diagram before you
implement them in code.
- Leave yourself lots of time for testing, or better yet,
create tests incrementally to test the functionality of
what you have so far. You *will* discover issues as part of testing,
and even with lots of modern tools, distributed debugging is
challenging.
- Please ask questions! We've done our best to make this an
intuitive and useful tool, but we are not infallible, so
please reach out to course staff if there is something we can
help clarify.
