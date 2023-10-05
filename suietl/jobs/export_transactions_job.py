import json

from ethereumetl.executors.batch_work_executor import BatchWorkExecutor
from blockchainetl.jobs.base_job import BaseJob

from suietl.json_rpc_requests import generate_multi_get_transaction_block_by_number_json_rpc
from suietl.mappers.objects_mapper import SuiObjectsMapper
from suietl.mappers.transaction_mapper import SuiTransactionMapper
from suietl.mappers.events_mapper import SuiEventsMapper
from suietl.utils import rpc_response_to_result

# https://docs.sui.io/sui-jsonrpc#sui_multiGetTransactionBlocks
# https://github.com/MystenLabs/sui/blob/67b5b8c11cdced5dac1d2bedb9179f1df7909537/crates/sui-rpc-loadgen/src/payload/validation.rs#L17
QUERY_MAX_RESULT_LIMIT = 25

# Exports transaction blocks and related objects
class ExportTransactionsJob(BaseJob):
    def __init__(
        self,
        transaction_hashes,
        batch_http_provider,
        max_workers,
        item_exporter,
    ):
        self.transaction_hashes = transaction_hashes

        self.batch_http_provider = batch_http_provider

        self.batch_work_executor = BatchWorkExecutor(QUERY_MAX_RESULT_LIMIT, max_workers)
        self.item_exporter = item_exporter

        self.objects_mapper = SuiObjectsMapper()
        self.transaction_mapper = SuiTransactionMapper()
        self.events_mapper = SuiEventsMapper()

    def _start(self):
        self.item_exporter.open()

    def _export(self):
        self.batch_work_executor.execute(
            self.transaction_hashes,
            self._export_batch,
            total_items=len(self.transaction_hashes),
        )

    def _export_batch(self, transaction_hashes):
        transaction_rpc = generate_multi_get_transaction_block_by_number_json_rpc(
            transaction_hashes
        )
        response = self.batch_http_provider.make_batch_request(
            json.dumps(transaction_rpc)
        )
        multiResult = rpc_response_to_result(response)

        for result in multiResult:
            # export transaction
            transaction = self.transaction_mapper.json_dict_to_transaction(result)
            self.item_exporter.export_item(
                self.transaction_mapper.transaction_to_dict(transaction)
            )

            # export objects
            objects = self.objects_mapper.json_dict_to_objects(result)
            for object in objects:
                self.item_exporter.export_item(
                    self.objects_mapper.object_to_dict(object)
                )

            # export events
            events = self.events_mapper.json_dict_to_events(result)
            for event in events:
                self.item_exporter.export_item(
                    self.events_mapper.event_to_dict(event)
                )

    def _end(self):
        self.batch_work_executor.shutdown()
        self.item_exporter.close()
