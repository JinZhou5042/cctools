"""Controller worker and replica-directory operations."""


class ReplicaStateMixin:
    def join_worker(self, worker_id, epoch):
        with self._lock:
            return self.replicas.join_worker(worker_id, epoch)

    def claim_worker(self, worker_id):
        with self._lock:
            return self.replicas.claim_worker(worker_id)

    def disconnect_worker(self, worker_id, epoch):
        with self._lock:
            return self.replicas.disconnect_worker(worker_id, epoch)

    def reconcile_workers(self, active_worker_ids):
        with self._lock:
            return self.replicas.reconcile_workers(active_worker_ids)

    def acquire_replica(
        self,
        data_id,
        replica_id,
        generation,
        destination_worker_id,
        destination_worker_epoch,
    ):
        with self._lock:
            return self.replicas.acquire_source(
                data_id,
                replica_id,
                generation,
                destination_worker_id,
                destination_worker_epoch,
            )

    def acquire_observed_transfer(
        self,
        data_id,
        source_worker_id,
        destination_worker_id,
        transfer_id,
    ):
        with self._lock:
            return self.replicas.acquire_observed_transfer(
                data_id,
                source_worker_id,
                destination_worker_id,
                transfer_id,
            )

    def release_replica(self, lease_id, success):
        with self._lock:
            return self.replicas.release_source(lease_id, success)

    def invalidate_worker_replica(
        self,
        data_id,
        replica_id,
        generation,
        worker_id,
        worker_epoch,
    ):
        with self._lock:
            return self.replicas.invalidate_worker_replica(
                data_id,
                replica_id,
                generation,
                worker_id,
                worker_epoch,
            )

    def invalidate_observed_worker_replica(
        self,
        data_id,
        replica_id,
        attempt,
        content_hash,
        size,
        worker_id,
        worker_epoch,
    ):
        with self._lock:
            return self.replicas.invalidate_observed_worker_replica(
                data_id,
                replica_id,
                attempt,
                content_hash,
                size,
                worker_id,
                worker_epoch,
            )

    def confirm_worker_pruned(
        self, data_id, replica_id, generation
    ):
        with self._lock:
            replica = self.replicas.confirm_worker_pruned(
                data_id, replica_id, generation
            )
            audit = self.pruning.audit(
                "confirm-worker-pruned",
                int(str(data_id).split(":", 1)[1]),
                "worker-cache-unlink-acknowledged",
                self.replicas.revision,
                replica.replica_id,
                replica.generation,
            )
            return {
                "replica": replica.source_dict(),
                "audit": audit.to_dict(),
            }

    def _validate_replica_identity(
        self, data_key, attempt, content_hash, size
    ):
        try:
            kind, token = str(data_key).split(":", 1)
            data_id = int(token)
        except (TypeError, ValueError):
            raise ValueError("invalid qualified DataID") from None
        attempt = int(attempt)
        size = int(size)
        if kind == "e":
            record = self.get_edata(data_id)
            expected_attempt = 1
            expected_hash = record.content_hash
            expected_size = record.serialized_size
        elif kind == "i":
            record = self.get_idata(data_id)
            expected_attempt = record.attempt
            expected_hash = record.content_hash
            expected_size = record.serialized_size
        else:
            raise ValueError("invalid qualified DataID")
        if (
            attempt != expected_attempt
            or content_hash != expected_hash
            or size != expected_size
        ):
            raise ValueError("replica does not match logical data identity")
        return record

    def prepare_worker_replica(
        self,
        data_key,
        replica_id,
        attempt,
        tier,
        content_hash,
        size,
        worker_id,
        worker_epoch,
    ):
        with self._lock:
            self._validate_replica_identity(
                data_key, attempt, content_hash, size
            )
            if tier not in ("worker-dram", "worker-disk"):
                raise ValueError("worker report requires worker tier")
            return self.replicas.prepare_replica(
                data_key,
                replica_id,
                attempt,
                tier,
                content_hash,
                size,
                worker_id,
                worker_epoch,
            )

    def commit_worker_replica(
        self,
        data_key,
        replica_id,
        generation,
        attempt,
        content_hash,
        size,
    ):
        with self._lock:
            self._validate_replica_identity(
                data_key, attempt, content_hash, size
            )
            replica = self.replicas.commit_replica(
                data_key,
                replica_id,
                generation,
                attempt,
                content_hash,
                size,
            )
            if data_key.startswith("i:"):
                value = self.get_idata(int(data_key.split(":", 1)[1]))
                self.pruning.set_data_state(
                    value.data_id,
                    available=True,
                    durable=value.durability == "durable",
                )
            return replica

    def prepare_worker_outputs(self, worker_id, worker_epoch, outputs):
        with self._lock:
            prepared = []
            for output in outputs:
                data_id = int(output["data_id"])
                record = self.publish_idata_metadata(
                    data_id,
                    output["attempt"],
                    output["content_hash"],
                    output["size"],
                )
                replica = self.prepare_worker_replica(
                    f"i:{data_id}",
                    output["replica_id"],
                    record.attempt,
                    "worker-disk",
                    record.content_hash,
                    record.serialized_size,
                    worker_id,
                    worker_epoch,
                )
                prepared.append(replica)
            return tuple(prepared)

    def commit_worker_outputs(self, outputs):
        with self._lock:
            return tuple(
                self.commit_worker_replica(
                    output["data_id"],
                    output["replica_id"],
                    output["generation"],
                    output["attempt"],
                    output["content_hash"],
                    output["size"],
                )
                for output in outputs
            )

    def report_worker_replica(
        self,
        data_key,
        replica_id,
        attempt,
        tier,
        content_hash,
        size,
        worker_id,
        worker_epoch,
    ):
        replica = self.prepare_worker_replica(
            data_key,
            replica_id,
            attempt,
            tier,
            content_hash,
            size,
            worker_id,
            worker_epoch,
        )
        return self.commit_worker_replica(
            data_key,
            replica_id,
            replica.generation,
            attempt,
            content_hash,
            size,
        )
