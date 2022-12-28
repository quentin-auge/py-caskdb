#!/usr/bin/env python

import logging
import time

from disk_store import DiskStorage
import random

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


t = time.time()

def random_str(k: int = 2):
    return f"{random.randint(0, 16 ** k - 1):x}".zfill(k)


db = DiskStorage("db")

n_records = 0

while True:
    key, value = random_str(), random_str()
    db[key] = value
    n_records += 1
    if n_records % int(1e6) == 0:
        LOGGER.info(f"Write throughput: {int(n_records / 1e3 / (time.time() - t))}k records / s")

