# MIT License
#
# Copyright (c) 2020 Evgeny Medvedev, evge.medvedev@gmail.com
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
import logging
from collections import defaultdict

from google.cloud import storage


def build_checkpoint_bundles(items):
    checkpoints = defaultdict(list)
    transactions = defaultdict(list)
    effects = defaultdict(list)
    events = defaultdict(list)
    for item in items:
        item_type = item.get("type")
        if item_type == "checkpoint":
            checkpoints[item.get("sequence_number")].append(item)
        elif item_type == "transaction":
            transactions[item.get("checkpoint_number")].append(item)
        elif item_type == "effect":
            effects[item.get("checkpoint_number")].append(item)
        elif item_type == "event":
            events[item.get("checkpoint_number")].append(item)
        else:
            logging.info(f"Skipping item with type {item_type}")

    checkpoint_bundles = []
    for checkpoint_number in sorted(checkpoints.keys()):
        if len(checkpoints[checkpoint_number]) != 1:
            raise ValueError(
                f"There must be a single checkpoint for a given checkpoint sequence number, was {len(checkpoints[checkpoint_number])} for checkpoint number {checkpoint_number}"
            )
        checkpoint_bundles.append(
            {
                "checkpoint": checkpoints[checkpoint_number][0],
                "transactions": transactions[checkpoint_number],
                "effect": effects[checkpoint_number],
                "events": events[checkpoint_number],
            }
        )

    return checkpoint_bundles


# TODO naman, do we build items on a transaction block level or checkpoint level?
# if it is on checkpoint, it can have multiple transactions that might cross partition time boundary.
# if it is on transaction block, then where does checkpoint go?
# picked checkpoint level for now
class GcsItemExporter:
    def __init__(
        self,
        bucket,
        path="checkpoints",
        build_checkpoint_bundles=build_checkpoint_bundles,
    ):
        self.bucket = bucket
        self.path = normalize_path(path)
        self.build_checkpoint_bundles = build_checkpoint_bundles
        self.storage_client = storage.Client()

    def open(self):
        pass

    def export_items(self, items):
        checkpoint_bundles = self.build_checkpoint_bundles(items)

        for checkpoint_bundle in checkpoint_bundles:
            checkpoint = checkpoint_bundle.get("checkpoint")
            if checkpoint is None:
                raise ValueError("checkpoint_bundle must include the checkpoint field")
            checkpoint_number = checkpoint.get("sequence_number")
            if checkpoint_number is None:
                raise ValueError(
                    "checkpoint_bundle must include the checkpoint.sequence_number field"
                )

            destination_blob_name = f"{self.path}/{checkpoint_number}.json"

            bucket = self.storage_client.bucket(self.bucket)
            blob = bucket.blob(destination_blob_name)
            blob.upload_from_string(json.dumps(checkpoint_bundle))
            logging.info(f"Uploaded file gs://{self.bucket}/{destination_blob_name}")

    def close(self):
        pass


def normalize_path(p):
    if p is None:
        p = ""
    if p.startswith("/"):
        p = p[1:]
    if p.endswith("/"):
        p = p[: len(p) - 1]

    return p
