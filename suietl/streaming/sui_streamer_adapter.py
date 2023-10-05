import json
import logging

from blockchainetl.jobs.exporters.console_item_exporter import ConsoleItemExporter
from blockchainetl.jobs.exporters.in_memory_item_exporter import InMemoryItemExporter

from suietl.enumeration.entity_type import EntityType
from suietl.jobs.export_checkpoints_job import ExportCheckpointsJob
from suietl.jobs.export_transactions_job import ExportTransactionsJob
from suietl.json_rpc_requests import generate_sui_get_latest_checkpoint_sequence_number
from suietl.streaming.sui_item_id_calculator import SuiItemIdCalculator
from suietl.streaming.sui_item_timestamp_calculator import SuiItemTimestampCalculator
from suietl.utils import rpc_response_to_result


class SuiStreamerAdapter:
    def __init__(
        self,
        batch_http_provider,
        item_exporter=ConsoleItemExporter(),
        batch_size=100,
        max_workers=5,
        entity_types=tuple(EntityType.ALL_FOR_STREAMING),
    ):
        self.batch_http_provider = batch_http_provider
        self.item_exporter = item_exporter
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.entity_types = entity_types
        self.item_id_calculator = SuiItemIdCalculator()
        self.item_timestamp_calculator = SuiItemTimestampCalculator()

    def open(self):
        self.item_exporter.open()

    def get_current_block_number(self):
        # SUI blocks don't have a block number, so we use checkpoints.
        rpc = generate_sui_get_latest_checkpoint_sequence_number()
        response = self.batch_http_provider.make_batch_request(json.dumps(rpc))
        return int(rpc_response_to_result(response))

    def export_all(self, start_block, end_block):
        # Since we use checkpoints, each checkpoint will have multiple blocks.
        # We make sure to not fetch more than one checkpoint at a time.
        assert start_block == end_block

        # Export checkpoints
        checkpoint = None
        if self._should_export(EntityType.CHECKPOINT):
            checkpoint = self._export_checkpoint(start_block)

        # Export transaction blocks and events
        transactions, objects, events = [], [], []
        if self._should_export(EntityType.TRANSACTION):
            transactions, objects, events = self._export_transaction_blocks(checkpoint)

        # enriched_blocks = blocks if EntityType.BLOCK in self.entity_types else []
        # enriched_transactions = (
        #     enrich_transactions(transactions, receipts)
        #     if EntityType.TRANSACTION in self.entity_types
        #     else []
        # )

        logging.info("Exporting with " + type(self.item_exporter).__name__)

        all_items = (
            sort_by([checkpoint], ("sequence_number",))
            + sort_by(transactions, ("checkpoint_sequence_number", "digest"))
            + sort_by(objects, ("checkpoint_sequence_number", "object_id", "version"))
            + sort_by(events, ("checkpoint_sequence_number", "transaction_digest", "event_seq"))
        )
        
        self.calculate_item_ids(all_items)
        self.calculate_item_timestamps(all_items)

        self.item_exporter.export_items(all_items)

    def _export_checkpoint(self, checkpoint_number):
        checkpoints_item_exporter = InMemoryItemExporter(item_types=["checkpoint"])
        checkpoints_job = ExportCheckpointsJob(
            checkpoint_number=checkpoint_number,
            batch_size=self.batch_size,
            batch_http_provider=self.batch_http_provider,
            max_workers=self.max_workers,
            item_exporter=checkpoints_item_exporter,
        )
        checkpoints_job.run()
        checkpoints = checkpoints_item_exporter.get_items("checkpoint")
        return checkpoints[0]

    def _export_transaction_blocks(self, checkpoint):
        transactions_item_exporter = InMemoryItemExporter(
            item_types=["transaction", "object", "event"]
        )
        transactions_job = ExportTransactionsJob(
            transaction_hashes=checkpoint["transactions"],
            batch_http_provider=self.batch_http_provider,
            max_workers=self.max_workers,
            item_exporter=transactions_item_exporter,
        )
        transactions_job.run()
        transactions = transactions_item_exporter.get_items("transaction")
        objects = transactions_item_exporter.get_items("object")
        events = transactions_item_exporter.get_items("event")
        return transactions, objects, events

    def _should_export(self, entity_type):
        if entity_type == EntityType.CHECKPOINT:
            return True

        if entity_type == EntityType.TRANSACTION:
            return EntityType.TRANSACTION in self.entity_types or self._should_export(
                EntityType.LOG
            )

        raise ValueError("Unexpected entity type " + entity_type)

    def calculate_item_ids(self, items):
        for item in items:
            item['item_id'] = self.item_id_calculator.calculate(item)

    def calculate_item_timestamps(self, items):
        for item in items:
            item['item_timestamp'] = self.item_timestamp_calculator.calculate(item)

    def close(self):
        self.item_exporter.close()


def sort_by(arr, fields):
    if isinstance(fields, tuple):
        fields = tuple(fields)
    return sorted(arr, key=lambda item: tuple(item.get(f) for f in fields))
