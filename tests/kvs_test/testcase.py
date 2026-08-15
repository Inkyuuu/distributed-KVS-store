import os
import traceback
from typing import Callable


class TestCase:
    def __init__(self, name: str, fn: Callable, weight: int = 1):
        self.name = name
        self.run = fn
        self.weight = weight

        self.score = None
        self.reason = None

    def execute(self, *args, **kwargs):
        # expect to receive a pass/fail and a reason
        try:
            self.score, self.reason = self.run(*args, **kwargs)
        except Exception as e:
            self.score = False
            self.reason = f"FAIL: {e}"
            if os.environ.get("KVS_TRACEBACK") == "1":
                traceback.print_exc()

        return self.score, self.reason

    def __str__(self):
        return f"{self.name}: {self.score} ({self.reason})"
