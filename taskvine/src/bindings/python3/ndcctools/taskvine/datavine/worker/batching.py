"""Synchronous worker-side coalescing for Controller metadata commits."""

import queue
import threading
import time


class _Publication:
    __slots__ = ("outputs", "result", "error", "done")

    def __init__(self, outputs):
        self.outputs = tuple(outputs)
        self.result = None
        self.error = None
        self.done = threading.Event()


class OutputPublisher:
    def __init__(
        self,
        client,
        worker_id,
        worker_epoch,
        coalesce_seconds=0.001,
        max_batch=64,
    ):
        self.client = client
        self.worker_id = str(worker_id)
        self.worker_epoch = int(worker_epoch)
        self.coalesce_seconds = float(coalesce_seconds)
        self.max_batch = int(max_batch)
        self._queue = queue.Queue()
        self._thread = threading.Thread(
            target=self._run,
            name="datavine-output-publisher",
            daemon=True,
        )
        self._thread.start()

    def publish(self, outputs):
        publication = _Publication(outputs)
        self._queue.put(publication)
        publication.done.wait()
        if publication.error is not None:
            raise publication.error
        return publication.result

    def _run(self):
        while True:
            first = self._queue.get()
            if first is None:
                return
            publications = [first]
            deadline = time.monotonic() + self.coalesce_seconds
            while len(publications) < self.max_batch:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                try:
                    publications.append(
                        self._queue.get(timeout=remaining)
                    )
                except queue.Empty:
                    break
            outputs = [
                output
                for publication in publications
                for output in publication.outputs
            ]
            try:
                committed = self.client.publish_outputs(
                    self.worker_id,
                    self.worker_epoch,
                    outputs,
                )
                offset = 0
                for publication in publications:
                    count = len(publication.outputs)
                    publication.result = committed[offset:offset + count]
                    offset += count
            except BaseException as exc:
                for publication in publications:
                    publication.error = exc
            finally:
                for publication in publications:
                    publication.done.set()

    def close(self):
        self._queue.put(None)
        self._thread.join(timeout=5)


class SourceResolver:
    def __init__(self, client, coalesce_seconds=0.001, max_batch=64):
        self.client = client
        self.coalesce_seconds = float(coalesce_seconds)
        self.max_batch = int(max_batch)
        self._queue = queue.Queue()
        self._thread = threading.Thread(
            target=self._run,
            name="datavine-source-resolver",
            daemon=True,
        )
        self._thread.start()

    def resolve(self, request):
        resolution = _Publication((request,))
        self._queue.put(resolution)
        resolution.done.wait()
        if resolution.error is not None:
            raise resolution.error
        return resolution.result

    def _run(self):
        while True:
            first = self._queue.get()
            if first is None:
                return
            resolutions = [first]
            deadline = time.monotonic() + self.coalesce_seconds
            while len(resolutions) < self.max_batch:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                try:
                    resolutions.append(
                        self._queue.get(timeout=remaining)
                    )
                except queue.Empty:
                    break
            try:
                results = self.client.resolve_edata_sources(
                    [item.outputs[0] for item in resolutions]
                )
                for resolution, result in zip(resolutions, results):
                    resolution.result = result
            except BaseException as exc:
                for resolution in resolutions:
                    resolution.error = exc
            finally:
                for resolution in resolutions:
                    resolution.done.set()

    def close(self):
        self._queue.put(None)
        self._thread.join(timeout=5)
