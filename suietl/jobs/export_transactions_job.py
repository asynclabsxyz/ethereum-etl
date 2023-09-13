# MIT License
#
# Copyright (c) 2018 Evgeny Medvedev, evge.medvedev@gmail.com
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


import json

from ethereumetl.executors.batch_work_executor import BatchWorkExecutor
from blockchainetl.jobs.base_job import BaseJob

from suietl.json_rpc_requests import generate_get_transaction_block_by_number_json_rpc
from suietl.mappers.transaction_mapper import SuiTransactionMapper
from suietl.mappers.transaction_block_effects_mapper import (
    SuiTransactionBlockEffectsMapper,
)
from suietl.mappers.transaction_block_events_mapper import (
    SuiTransactionBlockEventsMapper,
)
from suietl.utils import rpc_response_to_result, validate_checkpoint_number


# Exports transaction blocks and related objects
class ExportTransactionsJob(BaseJob):
    def __init__(
        self,
        transaction_hashes,
        batch_size,
        batch_http_provider,
        max_workers,
        item_exporter,
    ):
        self.transaction_hashes = transaction_hashes

        self.batch_http_provider = batch_http_provider

        self.batch_work_executor = BatchWorkExecutor(batch_size, max_workers)
        self.item_exporter = item_exporter

        self.transaction_mapper = SuiTransactionMapper()
        self.transaction_block_effects_mapper = SuiTransactionBlockEffectsMapper()
        self.transaction_block_events_mapper = SuiTransactionBlockEventsMapper()

    def _start(self):
        self.item_exporter.open()

    def _export(self):
        self.batch_work_executor.execute(
            range(0, len(self.transaction_hashes)),
            self._export_batch,
            total_items=len(self.transaction_hashes),
        )

    def _export_batch(self, transaction_hash_numbers_batch):
        for idx, transaction_hash_number in enumerate(transaction_hash_numbers_batch):
            transaction_rpc = generate_get_transaction_block_by_number_json_rpc(
                self.transaction_hashes[transaction_hash_number]
            )
            response = self.batch_http_provider.make_batch_request(
                json.dumps(transaction_rpc)
            )
            result = rpc_response_to_result(response)

            # export transaction
            transaction = self.transaction_mapper.json_dict_to_transaction(result)
            self.item_exporter.export_item(
                self.transaction_mapper.transaction_to_dict(transaction)
            )

            # export effects
            effects = self.transaction_block_effects_mapper.json_dict_to_effects(result)
            self.item_exporter.export_item(
                self.transaction_block_effects_mapper.effects_to_dict(effects)
            )

            # naman, this is not enabled yet because none of the blocks tested so far returns any events.
            # export events
            events = self.transaction_block_events_mapper.json_dict_to_events(result)
            for event in events:
                self.item_exporter.export_item(
                    self.transaction_block_events_mapper.event_to_dict(event)
                )

    def _end(self):
        self.batch_work_executor.shutdown()
        self.item_exporter.close()
