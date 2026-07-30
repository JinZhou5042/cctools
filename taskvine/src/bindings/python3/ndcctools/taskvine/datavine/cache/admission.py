"""Observed worker-cache retention and acknowledged eviction policy."""


class WorkerCacheAdmission:
    def __init__(self, controller):
        self.controller = controller
        self.clock = 0
        self.records = {}
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
                del self.records[key]

    def observe(self, record):
        self.clock += 1
        key = (str(record["worker_id"]), str(record["data_id"]))
        self.records[key] = {**record, "last_touch": self.clock}
        usage = self.usage()
        self.observed_bytes_high_water = max(
            self.observed_bytes_high_water,
            *(value["bytes"] for value in usage.values()),
        )
        self.observed_items_high_water = max(
            self.observed_items_high_water,
            *(value["items"] for value in usage.values()),
        )

    def usage(self):
        usage = {}
        for record in self.records.values():
            worker = usage.setdefault(
                record["worker_id"], {"bytes": 0, "items": 0}
            )
            worker["bytes"] += int(record["size"])
            worker["items"] += 1
        return usage

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
                del self.records[key]
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
        for worker_id in sorted(usage):
            projected_bytes = usage[worker_id]["bytes"]
            projected_items = usage[worker_id]["items"]
            candidates = [
                (key, record)
                for key, record in self.records.items()
                if key[0] == worker_id
                and key not in self.evictions
                and record["data_id"] not in self.prune_by_data
                and int(remaining_uses.get(record["data_id"], 0)) == 0
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
                invalidated = self.controller.invalidate_replica(
                    record["data_id"],
                    record["replica_id"],
                    record["generation"],
                    record["worker_id"],
                    record["worker_epoch"],
                )
                if invalidated["state"] == "retiring":
                    continue
                if invalidated["state"] not in ("invalid", "pruned"):
                    raise RuntimeError(
                        "cache eviction invalidation did not fail closed"
                    )
                if invalidated["state"] == "pruned":
                    del self.records[key]
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
