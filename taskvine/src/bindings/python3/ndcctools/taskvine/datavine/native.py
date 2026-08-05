"""Native Data Controller RPC client."""

import itertools
import json
import socket
import struct
import threading
import urllib.parse


_MAGIC = 0x44564331
_VERSION = 1
_AUTH = 1
_ALLOCATE_BATCH = 3
_PUBLISH_BATCH = 4
_CLAIM_WORKER = 5
_PUBLISH_OUTPUTS = 6
_DISCONNECT_WORKER = 7
_REPORT_REPLICA = 8
_RESOLVE_SOURCE = 9
_RELEASE_SOURCE = 10
_REGISTER_EDATA = 11
_GET_EDATA = 12
_INVALIDATE_REPLICA = 13
_RESTORE_REPLICA = 14
_CONFIRM_REPLICA_PRUNED = 15
_MARK_EDATA_SHARED = 16
_OK = 0
_REJECTED = 3
_MAX_BODY = 64 * 1024 * 1024


class NativeControllerError(RuntimeError):
    def __init__(self, status, opcode):
        super().__init__(
            f"native Controller opcode {opcode} failed with status {status}"
        )
        self.status = int(status)
        self.opcode = int(opcode)


class NativeControllerClient:
    def __init__(self, endpoint, token, timeout=30):
        parsed = urllib.parse.urlsplit(endpoint)
        if parsed.scheme != "tcp" or not parsed.hostname or not parsed.port:
            raise ValueError("native Controller endpoint must use tcp://host:port")
        self.endpoint = endpoint
        self.host = parsed.hostname
        self.port = parsed.port
        self.token = str(token).encode("utf-8")
        self.timeout = float(timeout)
        self._local = threading.local()
        self._request_ids = itertools.count(1)
        self._request_id_lock = threading.Lock()
        self._worker_endpoints = {}
        self._worker_epochs = {}

    @staticmethod
    def _read(stream, size):
        chunks = []
        remaining = int(size)
        while remaining:
            chunk = stream.recv(remaining)
            if not chunk:
                raise ConnectionError("native Controller connection closed")
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)

    def _connect(self):
        stream = socket.create_connection(
            (self.host, self.port), self.timeout
        )
        stream.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        stream.settimeout(self.timeout)
        self._local.stream = stream
        self._exchange(stream, _AUTH, self.token)
        return stream

    def _exchange(self, stream, opcode, payload):
        with self._request_id_lock:
            request_id = next(self._request_ids)
        stream.sendall(
            struct.pack(
                "!IHHIQ",
                _MAGIC,
                _VERSION,
                int(opcode),
                len(payload),
                request_id,
            )
            + payload
        )
        header = self._read(stream, 24)
        magic, version, response_opcode, status, size, response_id = (
            struct.unpack("!IHHIIQ", header)
        )
        if (
            magic != _MAGIC
            or version != _VERSION
            or response_opcode != opcode
            or response_id != request_id
            or size > _MAX_BODY
        ):
            raise RuntimeError("invalid native Controller response")
        body = self._read(stream, size) if size else b""
        if status != _OK:
            raise NativeControllerError(status, opcode)
        return body

    def request(self, opcode, payload=b""):
        stream = getattr(self._local, "stream", None)
        if stream is None:
            stream = self._connect()
        try:
            return self._exchange(stream, opcode, payload)
        except NativeControllerError:
            raise
        except (OSError, ConnectionError, RuntimeError):
            try:
                stream.close()
            finally:
                self._local.stream = None
            return self._exchange(self._connect(), opcode, payload)

    @staticmethod
    def _data_id(value):
        token = str(value)
        return int(token.split(":", 1)[-1])

    def allocate_idata(self, records):
        records = tuple(records)
        payload = [struct.pack("!I", len(records))]
        payload.extend(
            struct.pack("!qqiI", int(data_id), int(task_id), int(output), 0)
            for data_id, task_id, output in records
        )
        body = self.request(_ALLOCATE_BATCH, b"".join(payload))
        if len(body) != 4 or struct.unpack("!I", body)[0] != len(records):
            raise RuntimeError("native Controller returned incomplete allocation")

    def publish_idata_metadata(self, records):
        records = tuple(records)
        payload = [struct.pack("!I", len(records))]
        payload.extend(
            struct.pack(
                "!qI4xq64s",
                int(data_id),
                int(attempt),
                int(size),
                str(content_hash).encode("ascii"),
            )
            for data_id, attempt, content_hash, size in records
        )
        body = self.request(_PUBLISH_BATCH, b"".join(payload))
        if len(body) != 4 or struct.unpack("!I", body)[0] != len(records):
            raise RuntimeError("native Controller returned incomplete publication")

    def register_edata(self, records):
        batch = []
        batch_size = 4
        data_ids = []
        for record in records:
            metadata = json.dumps(
                record[0], sort_keys=True, separators=(",", ":")
            ).encode("ascii")
            inline = record[3] is not None
            payload = bytes(record[3]) if inline else b""
            size = int(record[4])
            encoded = (
                struct.pack(
                    "!IIQ64s64s",
                    int(inline),
                    len(metadata),
                    size,
                    str(record[1]).encode("ascii"),
                    str(record[2]).encode("ascii"),
                )
                + metadata
                + payload
            )
            if len(encoded) + 4 > _MAX_BODY:
                raise ValueError("EData exceeds native Controller RPC limit")
            if batch and batch_size + len(encoded) > _MAX_BODY:
                data_ids.extend(self._register_edata_batch(batch))
                batch = []
                batch_size = 4
            batch.append(encoded)
            batch_size += len(encoded)
        if batch:
            data_ids.extend(self._register_edata_batch(batch))
        return tuple(data_ids)

    def _register_edata_batch(self, records):
        body = self.request(
            _REGISTER_EDATA,
            struct.pack("!I", len(records)) + b"".join(records),
        )
        if len(body) != 4 + 8 * len(records) or struct.unpack_from(
            "!I", body
        )[0] != len(records):
            raise RuntimeError("native Controller returned incomplete EData")
        return struct.unpack_from(f"!{len(records)}q", body, 4)

    def get_edata(self, data_id, allow_shared=False, metadata_only=False):
        request = struct.pack("!q", int(data_id))
        options = int(bool(allow_shared)) | (int(bool(metadata_only)) << 1)
        if options:
            request += struct.pack("!I", options)
        body = self.request(_GET_EDATA, request)
        if len(body) < 148:
            raise RuntimeError("native Controller returned invalid EData")
        serialized_size, payload_size, cache_globally = struct.unpack_from(
            "!QQI", body
        )
        if len(body) != 148 + payload_size:
            raise RuntimeError("native Controller returned truncated EData")
        return {
            "content_hash": body[20:84].decode("ascii"),
            "serialized_sha256": body[84:148].decode("ascii"),
            "cache_globally": bool(cache_globally),
            "serialized_size": serialized_size,
            "payload": body[148:],
        }

    def mark_edata_shared(self, data_ids):
        data_ids = tuple(map(int, data_ids))
        body = self.request(
            _MARK_EDATA_SHARED,
            struct.pack("!I", len(data_ids))
            + b"".join(struct.pack("!q", value) for value in data_ids),
        )
        if len(body) != 4 or struct.unpack("!I", body)[0] != len(data_ids):
            raise RuntimeError("native Controller returned incomplete policy update")

    def claim_worker(self, worker_id, endpoint):
        worker = str(worker_id).encode("utf-8")
        address = str(endpoint).encode("utf-8")
        body = self.request(
            _CLAIM_WORKER,
            struct.pack("!HH", len(worker), len(address)) + worker + address,
        )
        if len(body) != 8:
            raise RuntimeError("native Controller returned invalid worker epoch")
        epoch = struct.unpack("!Q", body)[0]
        with self._request_id_lock:
            self._worker_endpoints[str(worker_id)] = address
            self._worker_epochs[str(worker_id)] = epoch
        return epoch

    def _worker_endpoint(self, worker_id):
        with self._request_id_lock:
            endpoint = self._worker_endpoints.get(str(worker_id))
        if endpoint is None:
            raise RuntimeError("worker data endpoint has not been claimed")
        return endpoint

    def worker_endpoint(self, worker_id):
        return self._worker_endpoint(worker_id).decode("utf-8")

    def worker_epoch(self, worker_id):
        with self._request_id_lock:
            return self._worker_epochs.get(str(worker_id))

    def disconnect_worker(self, worker_id, epoch):
        worker = str(worker_id).encode("utf-8")
        self.request(
            _DISCONNECT_WORKER,
            struct.pack("!H2xQ", len(worker), int(epoch)) + worker,
        )

    def publish_outputs(self, worker_id, worker_epoch, outputs):
        worker = str(worker_id).encode("utf-8")
        endpoint = self._worker_endpoint(worker_id)
        outputs = tuple(outputs)
        payload = [
            struct.pack(
                "!HHQI",
                len(worker),
                len(endpoint),
                int(worker_epoch),
                len(outputs),
            ),
            worker,
            endpoint,
        ]
        for output in outputs:
            payload.append(
                struct.pack(
                    "!qiIq64s",
                    self._data_id(output["data_id"]),
                    int(output["attempt"]),
                    2,
                    int(output["size"]),
                    str(output["content_hash"]).encode("ascii"),
                )
            )
        body = self.request(_PUBLISH_OUTPUTS, b"".join(payload))
        if len(body) != 4 + 8 * len(outputs):
            raise RuntimeError("native Controller returned invalid publications")
        count = struct.unpack_from("!I", body)[0]
        if count != len(outputs):
            raise RuntimeError("native Controller returned incomplete publications")
        result = []
        for index, output in enumerate(outputs):
            data_id = self._data_id(output["data_id"])
            result.append(
                {
                    "data_id": f"i:{data_id}",
                    "replica_id": f"taskvine-{worker_id}-i-{data_id}",
                    "generation": struct.unpack_from(
                        "!Q", body, 4 + 8 * index
                    )[0],
                    "attempt": int(output["attempt"]),
                    "tier": "worker-disk",
                    "content_hash": str(output["content_hash"]),
                    "size": int(output["size"]),
                    "state": "available",
                    "load": 0,
                    "worker_id": str(worker_id),
                    "worker_epoch": int(worker_epoch),
                    "source_endpoint": endpoint.decode("utf-8"),
                }
            )
        return result

    def report_replica(
        self,
        data_id,
        replica_id,
        attempt,
        tier,
        content_hash,
        size,
        worker_id,
        worker_epoch,
    ):
        kind, token = str(data_id).split(":", 1)
        worker = str(worker_id).encode("utf-8")
        replica = str(replica_id).encode("utf-8")
        endpoint = self._worker_endpoint(worker_id)
        tier_id = {"worker-dram": 1, "worker-disk": 2}[str(tier)]
        payload = struct.pack(
            "!BBHHHQQiIq64s",
            ord(kind),
            tier_id,
            len(worker),
            len(replica),
            len(endpoint),
            int(worker_epoch),
            int(token),
            int(attempt),
            0,
            int(size),
            str(content_hash).encode("ascii"),
        ) + worker + replica + endpoint
        body = self.request(_REPORT_REPLICA, payload)
        if len(body) != 8:
            raise RuntimeError("native Controller returned invalid replica")
        return struct.unpack("!Q", body)[0]

    def resolve_source(
        self,
        data_id,
        destination_worker_id,
        destination_worker_epoch,
        transfer_id,
        excluded_worker_id=None,
    ):
        kind, token = str(data_id).split(":", 1)
        destination = str(destination_worker_id).encode("utf-8")
        transfer = str(transfer_id).encode("utf-8")
        excluded = (
            str(excluded_worker_id).encode("utf-8")
            if excluded_worker_id
            else b""
        )
        payload = struct.pack(
            "!BBHHHQQ",
            ord(kind),
            0,
            len(destination),
            len(transfer),
            len(excluded),
            int(destination_worker_epoch),
            int(token),
        ) + destination + transfer + excluded
        body = self.request(_RESOLVE_SOURCE, payload)
        if len(body) < 112:
            raise RuntimeError("native Controller returned invalid source")
        (
            generation,
            attempt,
            tier,
            size,
            load,
            _,
            worker_epoch,
            worker_length,
            replica_length,
            endpoint_length,
            transfer_length,
            content_hash,
        ) = struct.unpack_from("!QiiqIIQHHHH64s", body)
        if len(body) != (
            112
            + worker_length
            + replica_length
            + endpoint_length
            + transfer_length
        ):
            raise RuntimeError("native Controller returned malformed source")
        offset = 112

        def take(length):
            nonlocal offset
            value = body[offset:offset + length].decode("utf-8")
            offset += length
            return value

        source_worker = take(worker_length)
        replica_id = take(replica_length)
        endpoint = take(endpoint_length)
        lease_id = take(transfer_length)
        digest = content_hash.decode("ascii")
        source = {
            "data_id": f"{kind}:{int(token)}",
            "replica_id": replica_id,
            "generation": generation,
            "attempt": attempt,
            "tier": {1: "worker-dram", 2: "worker-disk"}[tier],
            "content_hash": digest,
            "size": size,
            "state": "available",
            "load": load,
            "worker_id": source_worker,
            "worker_epoch": worker_epoch,
            "source_url": (
                f"{endpoint}/data/{kind}/{int(token)}"
                f"?sha256={digest}&size={size}"
            ),
        }
        return {
            "source": source,
            "lease": {
                "lease_id": lease_id,
                "data_id": source["data_id"],
                "replica_id": replica_id,
                "generation": generation,
                "destination_worker_id": str(destination_worker_id),
                "destination_worker_epoch": int(destination_worker_epoch),
                "active": True,
                "success": None,
            },
        }

    def release_source(self, transfer_id, success):
        transfer = str(transfer_id).encode("utf-8")
        self.request(
            _RELEASE_SOURCE,
            struct.pack("!B3xH2x", bool(success), len(transfer)) + transfer,
        )

    def invalidate_replica(self, data_id, replica_id):
        self._change_replica(_INVALIDATE_REPLICA, data_id, replica_id)

    def restore_replica(self, data_id, replica_id):
        self._change_replica(_RESTORE_REPLICA, data_id, replica_id)

    def confirm_replica_pruned(self, data_id, replica_id):
        self._change_replica(
            _CONFIRM_REPLICA_PRUNED, data_id, replica_id
        )

    def _change_replica(self, opcode, data_id, replica_id):
        kind, token = str(data_id).split(":", 1)
        replica = str(replica_id).encode("utf-8")
        self.request(
            opcode,
            struct.pack("!cxH4xq", kind.encode("ascii"), len(replica), int(token))
            + replica,
        )
