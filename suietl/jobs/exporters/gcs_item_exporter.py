import json
import logging
from collections import defaultdict

from google.cloud import storage


def build_checkpoint_bundles(items):
    checkpoints = defaultdict(list)
    transactions = defaultdict(list)
    objects = defaultdict(list)
    events = defaultdict(list)
    for item in items:
        item_type = item.get("type")
        if item_type == "checkpoint":
            checkpoints[item.get("sequence_number")].append(item)
        elif item_type == "transaction":
            transactions[item.get("checkpoint_number")].append(item)
        elif item_type == "object":
            objects[item.get("checkpoint_sequence_number")].append(item)
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
                "objects": objects[checkpoint_number],
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
