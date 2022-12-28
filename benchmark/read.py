#!/usr/bin/env python

import logging
import time

from disk_store import DiskStorage
import random

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def random_str(k: int = 2):
    return f"{random.randint(0, 16 ** k - 1):x}".zfill(k)


t = time.time()

db = DiskStorage("db")
db["a"] = "b"

random.seed(3)

for _ in range(10):
    key = random_str()
    print(f"{key} = {db[key]}")
