"""Direct worker calls into the concurrent Controller."""


class OutputPublisher:
    def __init__(self, client, worker_id, worker_epoch):
        self.client = client
        self.worker_id = str(worker_id)
        self.worker_epoch = int(worker_epoch)

    def publish(self, outputs):
        committed = self.client.publish_outputs(
            self.worker_id, self.worker_epoch, outputs
        )
        if committed:
            self.worker_epoch = int(committed[0]["worker_epoch"])
        return committed

    def close(self):
        pass


class SourceResolver:
    def __init__(self, client):
        self.client = client

    def resolve(self, request):
        return self.client.resolve_edata_sources((request,))[0]

    def close(self):
        pass
