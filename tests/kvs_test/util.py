import subprocess
import sys
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Generator

LOG_BUFFER = ""


def log(*args):
    if (c := _log_capture.get()) is not None:
        c.log(*args)

    # log to stderr
    print(*args, file=sys.stderr)

    # log to buffer
    global LOG_BUFFER
    formatted = " ".join(map(str, args))
    LOG_BUFFER += formatted + "\n"


def log_buffer_reset():
    global LOG_BUFFER
    LOG_BUFFER = ""


def get_log_buffer():
    global LOG_BUFFER
    return LOG_BUFFER


@contextmanager
def capture_logs() -> Generator["LogCapture", None, None]:
    c = LogCapture()
    prev = _log_capture.set(c)
    yield c
    _log_capture.reset(prev)


class LogCapture:
    def __init__(self):
        self.buffer = ""

    def log(self, *args):
        # log to buffer
        formatted = " ".join(map(str, args))
        self.buffer += formatted + "\n"


_log_capture: ContextVar[LogCapture | None] = ContextVar("_log_interceptor", default=None)


def get_logger(prefix: str):
    return lambda *args: log(f"{prefix}: ", *args)


def run_cmd_bg(
    cmd: list[str], verbose=False, error_prefix: str = "command failed", **kwargs
) -> subprocess.CompletedProcess:
    # default capture opts
    kwargs.setdefault("stdout", subprocess.PIPE)
    kwargs.setdefault("stderr", subprocess.PIPE)
    kwargs.setdefault("text", True)
    kwargs.setdefault("check", True)

    if verbose:
        log(f"$ {cmd[0]} {' '.join(cmd[1:])}")

    try:
        return subprocess.run(cmd, **kwargs)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"{error_prefix}: {e}:\nstdout: {e.stdout}\nstderr: {e.stderr}")
