"""Direct worker calls into the concurrent Controller."""


class OutputPublisher:
    def __init__(self, client, worker_id, worker_epoch):
        self.client = client
        self.worker_id = str(worker_id)
        self.worker_epoch = int(worker_epoch)

    def publish(self, outputs):
        count, worker_epoch = self.client.publish_outputs(
            self.worker_id, self.worker_epoch, outputs
        )
        self.worker_epoch = int(worker_epoch)
        return count


class SourceResolver:
    def __init__(self, client):
        self.client = client

    def resolve(self, request):
        return self.client.resolve_edata_sources((request,))[0]
