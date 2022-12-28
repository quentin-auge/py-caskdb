"""
disk_store module implements DiskStorage class which implements the KV store on the
disk

DiskStorage provides two simple operations to get and set key value pairs. Both key and
value needs to be of string type. All the data is persisted to disk. During startup,
DiskStorage loads all the existing KV pair metadata.  It will throw an error if the
file is invalid or corrupt.

Do note that if the database file is large, then the initialisation will take time
accordingly. The initialisation is also a blocking operation, till it is completed
the DB cannot be used.

Typical usage example:

    disk: DiskStorage = DiskStore(file_name="books.db")
    disk.set(key="othello", value="shakespeare")
    author: str = disk.get("othello")
    # it also supports dictionary style API too:
    disk["hamlet"] = "shakespeare"
"""
import logging
import time
import typing

from format import encode_kv, decode_kv, decode_header, HEADER_SIZE

LOGGER = logging.getLogger(__name__)


# DiskStorage is a Log-Structured Hash Table as described in the BitCask paper. We
# keep appending the data to a file, like a log. DiskStorage maintains an in-memory
# hash table called KeyDir, which keeps the row's location on the disk.
#
# The idea is simple yet brilliant:
#   - Write the record to the disk
#   - Update the internal hash table to point to that byte offset
#   - Whenever we get a read request, check the internal hash table for the address,
#       fetch that and return
#
# KeyDir does not store values, only their locations.
#
# The above approach solves a lot of problems:
#   - Writes are insanely fast since you are just appending to the file
#   - Reads are insanely fast since you do only one disk seek. In B-Tree backed
#       storage, there could be 2-3 disk seeks
#
# However, there are drawbacks too:
#   - We need to maintain an in-memory hash table KeyDir. A database with a large
#       number of keys would require more RAM
#   - Since we need to build the KeyDir at initialisation, it will affect the startup
#       time too
#   - Deleted keys need to be purged from the file to reduce the file size
#
# Read the paper for more details: https://riak.com/assets/bitcask-intro.pdf


class DiskStorage:
    """
    Implements the KV store on the disk

    Args:
        file_name (str): name of the file where all the data will be written. Just
            passing the file name will save the data in the current directory. You may
            pass the full file location too.
    """

    def __init__(self, file_name: str = "data.db"):
        self.fw = open(file_name, "ab")
        self.fr = open(file_name, "rb")
        self.keydir = self._load_keydir(self.fr)

    @staticmethod
    def _load_keydir(f: typing.BinaryIO) -> typing.Dict[str, typing.Tuple[int, int]]:
        t = time.time()
        n_records = 0

        keydir = {}
        offset, header = f.tell(), f.read(HEADER_SIZE)

        while header:
            _, ksz, vsz = decode_header(header)
            key = f.read(ksz).decode("utf-8")
            _ = f.read(vsz)
            keydir[key] = (offset, HEADER_SIZE + ksz + vsz)
            offset, header = f.tell(), f.read(HEADER_SIZE)
            n_records += 1

        LOGGER.info(f"Loaded {(n_records / 1e6):.1f}M keydir records in {(time.time() - t):.2f}s")

        return keydir


    def set(self, key: str, value: str) -> None:
        timestamp = int(time.time())
        size, data = encode_kv(timestamp, key, value)
        self.keydir[key] = (self.fw.tell(), size)
        self.fw.write(data)
        self.fw.flush()

    def get(self, key: str) -> str:
        if key not in self.keydir:
            return ""
        offset, size = self.keydir[key]
        self.fr.seek(offset)
        _, _, value = decode_kv(self.fr.read(size))
        return value

    def close(self) -> None:
        self.fw.close()
        self.fr.close()

    def __setitem__(self, key: str, value: str) -> None:
        return self.set(key, value)

    def __getitem__(self, item: str) -> str:
        return self.get(item)
