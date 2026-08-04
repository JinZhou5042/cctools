"""Controller-authorized worker cache retention."""


class WorkerCacheAdmission:
    def __init__(self, controller):
        self.controller = controller
        self.clock = 0
        self.records = {}
        self.records_by_worker = {}
        self.usage_by_worker = {}
        self.eviction_count = 0
        self.eviction_records = []
        self.observed_bytes_high_water = 0
        self.observed_items_high_water = 0

    def sync_workers(self, worker_ids):
        worker_ids = set(worker_ids)
        for key in tuple(self.records):
            if key[0] not in worker_ids:
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

    def observe(self, record):
        self.clock += 1
        key = (str(record["worker_id"]), str(record["data_id"]))
        current = self.records.get(key)
        if current is not None:
            self._remove_record(key)
        record = {**record, "last_touch": self.clock}
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

    def usage(self):
        return {
            worker_id: dict(value)
            for worker_id, value in self.usage_by_worker.items()
        }

    def within_capacity(self, capacity_bytes, capacity_items):
        return all(
            (capacity_bytes is None or value["bytes"] <= int(capacity_bytes))
            and (capacity_items is None or value["items"] <= int(capacity_items))
            for value in self.usage_by_worker.values()
        )

    @staticmethod
    def _source_url(record):
        endpoint = record.get("source_endpoint")
        if not endpoint:
            raise RuntimeError(
                f"worker cache replica {record['replica_id']} lacks endpoint"
            )
        kind, data_id = record["data_id"].split(":", 1)
        return (
            f"{endpoint}/data/{kind}/{int(data_id)}"
            f"?sha256={record['content_hash']}&size={int(record['size'])}"
        )

    def enforce(
        self,
        capacity_bytes,
        capacity_items,
        remaining_uses,
        protected_data=(),
    ):
        if capacity_bytes is None and capacity_items is None:
            return
        byte_limit = None if capacity_bytes is None else int(capacity_bytes)
        item_limit = None if capacity_items is None else int(capacity_items)
        if byte_limit is not None and byte_limit < 0:
            raise ValueError("worker disk cache byte capacity is negative")
        if item_limit is not None and item_limit < 0:
            raise ValueError("worker disk cache item capacity is negative")
        protected_data = set(protected_data)
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

        for worker_id in sorted(self.usage_by_worker):
            usage = self.usage_by_worker.get(worker_id)
            if usage is None:
                continue
            candidates = [
                (key, self.records[key])
                for key in self.records_by_worker.get(worker_id, ())
                if self.records[key]["data_id"] not in protected_data
                and (
                    int(remaining_uses.get(self.records[key]["data_id"], 0))
                    == 0
                    or can_rematerialize(self.records[key]["data_id"])
                )
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
                (byte_limit is not None and usage["bytes"] > byte_limit)
                or (item_limit is not None and usage["items"] > item_limit)
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
                if invalidated["state"] == "retiring":
                    continue
                if invalidated["state"] not in ("invalid", "pruned"):
                    raise RuntimeError(
                        "cache eviction invalidation did not fail closed"
                    )
                self.controller.prune_source(self._source_url(record))
                if invalidated["state"] != "pruned":
                    self.controller.confirm_replica_pruned(
                        record["data_id"],
                        record["replica_id"],
                        invalidated["generation"],
                    )
                self._remove_record(key)
                self.eviction_count += 1
                self.eviction_records.append(
                    {
                        "data_id": record["data_id"],
                        "worker_id": worker_id,
                        "size": record["size"],
                        "remaining_uses": int(
                            remaining_uses.get(record["data_id"], 0)
                        ),
                        "outcome": "pruned",
                    }
                )

    def report(self, capacity_bytes, capacity_items):
        return {
            "worker_disk_cache_capacity_bytes": capacity_bytes,
            "worker_disk_cache_capacity_items": capacity_items,
            "worker_disk_cache_evictions": self.eviction_count,
            "worker_disk_cache_eviction_records": list(self.eviction_records),
            "worker_disk_cache_usage": self.usage(),
            "worker_disk_cache_observed_bytes_high_water": (
                self.observed_bytes_high_water
            ),
            "worker_disk_cache_observed_items_high_water": (
                self.observed_items_high_water
            ),
        }
