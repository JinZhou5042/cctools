"""Observed worker-cache retention and acknowledged eviction policy."""


class WorkerCacheAdmission:
    def __init__(self, controller):
        self.controller = controller
        self.clock = 0
        self.records = {}
        self.records_by_worker = {}
        self.usage_by_worker = {}
        self.evictions = {}
        self.prune_by_data = set()
        self.eviction_count = 0
        self.eviction_records = []
        self.observed_bytes_high_water = 0
        self.observed_items_high_water = 0
        self.active_worker_ids = set()

    def sync_workers(self, worker_ids):
        self.active_worker_ids = set(worker_ids)
        for key in tuple(self.records):
            if key[0] not in worker_ids and key not in self.evictions:
                self._remove_record(key)

    def _remove_record(self, key):
        record = self.records.pop(key)
        worker_id = record["worker_id"]
        usage = self.usage_by_worker[worker_id]
        usage["bytes"] -= int(record["size"])
        usage["items"] -= 1
        worker_records = self.records_by_worker[worker_id]
        worker_records.remove(key)
        if not worker_records:
            del self.records_by_worker[worker_id]
        if not usage["items"]:
            if usage["bytes"]:
                raise RuntimeError("worker cache byte accounting leaked")
            del self.usage_by_worker[worker_id]

    def _store_record(self, key, record):
        current = self.records.get(key)
        if current is not None:
            self._remove_record(key)
        worker_id = record["worker_id"]
        self.records[key] = record
        self.records_by_worker.setdefault(worker_id, set()).add(key)
        usage = self.usage_by_worker.setdefault(
            worker_id, {"bytes": 0, "items": 0}
        )
        usage["bytes"] += int(record["size"])
        usage["items"] += 1
        self.observed_bytes_high_water = max(
            self.observed_bytes_high_water, usage["bytes"]
        )
        self.observed_items_high_water = max(
            self.observed_items_high_water, usage["items"]
        )

    def observe(self, record):
        self.clock += 1
        key = (str(record["worker_id"]), str(record["data_id"]))
        self._store_record(
            key, {**record, "last_touch": self.clock}
        )

    def usage(self):
        return {
            worker_id: dict(value)
            for worker_id, value in self.usage_by_worker.items()
        }

    def within_capacity(self, capacity_bytes, capacity_items):
        if capacity_bytes is None and capacity_items is None:
            return True
        for usage in self.usage().values():
            if (
                capacity_bytes is not None
                and usage["bytes"] > int(capacity_bytes)
            ):
                return False
            if (
                capacity_items is not None
                and usage["items"] > int(capacity_items)
            ):
                return False
        return True

    def poll(self, manager):
        for key, pending in tuple(self.evictions.items()):
            status = manager.prune_file_status(pending["file"])
            confirmed = status["confirmed"] - pending["before"]["confirmed"]
            failed = status["failed"] - pending["before"]["failed"]
            if confirmed + failed < 1:
                continue
            if failed:
                if pending["worker_id"] in self.active_worker_ids:
                    raise RuntimeError(
                        "worker cache eviction failed for "
                        f"{pending['data_id']} on "
                        f"{pending['worker_id']}"
                    )
            else:
                self.controller.confirm_replica_pruned(
                    pending["data_id"],
                    pending["replica_id"],
                    pending["generation"],
                )
            current = self.records.get(key)
            if (
                current is not None
                and int(current["generation"])
                == int(pending["generation"])
            ):
                self._remove_record(key)
            del self.evictions[key]
            self.prune_by_data.remove(pending["data_id"])
            self.eviction_count += 1
            self.eviction_records.append(
                {
                    "data_id": pending["data_id"],
                    "worker_id": pending["worker_id"],
                    "size": pending["size"],
                    "remaining_uses": pending["remaining_uses"],
                    "outcome": (
                        "worker-lost" if failed else "pruned"
                    ),
                }
            )
            if not manager.forget_prune_file_status(pending["file"]):
                raise RuntimeError(
                    "completed cache eviction tracker did not release"
                )

    def enforce(
        self,
        manager,
        file_resolver,
        capacity_bytes,
        capacity_items,
        remaining_uses,
        protected_data=(),
    ):
        if capacity_bytes is None and capacity_items is None:
            return
        byte_limit = (
            None if capacity_bytes is None else int(capacity_bytes)
        )
        item_limit = (
            None if capacity_items is None else int(capacity_items)
        )
        if byte_limit is not None and byte_limit < 0:
            raise ValueError("worker disk cache byte capacity is negative")
        if item_limit is not None and item_limit < 0:
            raise ValueError("worker disk cache item capacity is negative")
        self.poll(manager)
        usage = self.usage()
        rematerializable = {}

        def can_rematerialize(data_key):
            if data_key.startswith("e:"):
                return True
            if data_key not in rematerializable:
                status = self.controller.idata_status(
                    int(data_key.split(":", 1)[1])
                )
                rematerializable[data_key] = bool(
                    status["rematerializable"]
                )
            return rematerializable[data_key]

        for worker_id in sorted(usage):
            projected_bytes = usage[worker_id]["bytes"]
            projected_items = usage[worker_id]["items"]
            for pending in self.evictions.values():
                if pending["worker_id"] != worker_id:
                    continue
                projected_bytes -= int(pending["size"])
                projected_items -= 1
            if (
                (
                    byte_limit is None
                    or projected_bytes <= byte_limit
                )
                and (
                    item_limit is None
                    or projected_items <= item_limit
                )
            ):
                continue
            candidates = [
                (key, record)
                for key in self.records_by_worker.get(worker_id, ())
                for record in (self.records[key],)
                if key[0] == worker_id
                and key not in self.evictions
                and record["data_id"] not in self.prune_by_data
                and record["data_id"] not in protected_data
                and (
                    int(
                        remaining_uses.get(record["data_id"], 0)
                    ) == 0
                    or can_rematerialize(record["data_id"])
                )
                and file_resolver(record["data_id"]) is not None
            ]
            candidates.sort(
                key=lambda item: (
                    int(remaining_uses.get(item[1]["data_id"], 0) > 0),
                    int(remaining_uses.get(item[1]["data_id"], 0)),
                    -int(item[1]["size"]),
                    int(item[1]["last_touch"]),
                    item[1]["data_id"],
                )
            )
            while (
                (
                    byte_limit is not None
                    and projected_bytes > byte_limit
                )
                or (
                    item_limit is not None
                    and projected_items > item_limit
                )
            ):
                if not candidates:
                    break
                key, record = candidates.pop(0)
                invalidated = self.controller.invalidate_observed_replica(
                    record["data_id"],
                    record["replica_id"],
                    record["attempt"],
                    record["content_hash"],
                    record["size"],
                    record["worker_id"],
                    record["worker_epoch"],
                )
                record = {
                    **record,
                    "generation": invalidated["generation"],
                }
                self.records[key] = record
                if invalidated["state"] == "retiring":
                    continue
                if invalidated["state"] not in ("invalid", "pruned"):
                    raise RuntimeError(
                        "cache eviction invalidation did not fail closed"
                    )
                if invalidated["state"] == "pruned":
                    self._remove_record(key)
                    projected_bytes -= int(record["size"])
                    projected_items -= 1
                    continue
                file_object = file_resolver(record["data_id"])
                before = manager.prune_file_status(file_object)
                requested = manager.prune_file_on_worker(
                    file_object, worker_id
                )
                if requested != 1:
                    continue
                self.evictions[key] = {
                    **record,
                    "file": file_object,
                    "before": before,
                    "remaining_uses": int(
                        remaining_uses.get(record["data_id"], 0)
                    ),
                }
                self.prune_by_data.add(record["data_id"])
                projected_bytes -= int(record["size"])
                projected_items -= 1

    def report(self, capacity_bytes, capacity_items):
        return {
            "worker_disk_cache_capacity_bytes": capacity_bytes,
            "worker_disk_cache_capacity_items": capacity_items,
            "worker_disk_cache_evictions": self.eviction_count,
            "worker_disk_cache_eviction_records": list(
                self.eviction_records
            ),
            "worker_disk_cache_usage": self.usage(),
            "worker_disk_cache_observed_bytes_high_water": (
                self.observed_bytes_high_water
            ),
            "worker_disk_cache_observed_items_high_water": (
                self.observed_items_high_water
            ),
        }
