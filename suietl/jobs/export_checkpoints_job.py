import json

from ethereumetl.executors.batch_work_executor import BatchWorkExecutor
from blockchainetl.jobs.base_job import BaseJob

from suietl.json_rpc_requests import generate_get_checkpoint_by_number_json_rpc
from suietl.mappers.checkpoint_mapper import SuiCheckpointMapper
from suietl.utils import rpc_response_to_result, validate_checkpoint_number


# Exports checkpoints
class ExportCheckpointsJob(BaseJob):
    def __init__(
        self,
        checkpoint_number,
        batch_size,
        batch_http_provider,
        max_workers,
        item_exporter,
    ):
        validate_checkpoint_number(checkpoint_number)
        self.checkpoint_number = checkpoint_number

        self.batch_http_provider = batch_http_provider

        self.batch_work_executor = BatchWorkExecutor(batch_size, max_workers)
        self.item_exporter = item_exporter

        self.checkpoint_mapper = SuiCheckpointMapper()

    def _start(self):
        self.item_exporter.open()

    def _export(self):
        self.batch_work_executor.execute(
            range(self.checkpoint_number, self.checkpoint_number + 1),
            self._export_batch,
            total_items=1,
        )

    def _export_batch(self, checkpoint_number_batch):
        for idx, checkpoint_number in enumerate(checkpoint_number_batch):
            checkpoint_rpc = generate_get_checkpoint_by_number_json_rpc(
                checkpoint_number
            )
            response = self.batch_http_provider.make_batch_request(
                json.dumps(checkpoint_rpc)
            )
            result = rpc_response_to_result(response)
            checkpoint = self.checkpoint_mapper.json_dict_to_block(result)

            self.item_exporter.export_item(
                self.checkpoint_mapper.checkpoint_to_dict(checkpoint)
            )

    def _end(self):
        self.batch_work_executor.shutdown()
        self.item_exporter.close()
