from blockchainetl.jobs.exporters.console_item_exporter import ConsoleItemExporter
from blockchainetl.jobs.exporters.multi_item_exporter import MultiItemExporter
from ethereumetl.streaming.item_exporter_creator import (
    get_bucket_and_path_from_gcs_output,
    determine_item_exporter_type,
    ItemExporterType,
)


def create_item_exporters(outputs):
    split_outputs = (
        [output.strip() for output in outputs.split(",")] if outputs else ["console"]
    )

    item_exporters = [create_item_exporter(output) for output in split_outputs]
    return MultiItemExporter(item_exporters)


def create_item_exporter(output):
    item_exporter_type = determine_item_exporter_type(output)
    if item_exporter_type == ItemExporterType.GCS:
        from suietl.jobs.exporters.gcs_item_exporter import GcsItemExporter

        bucket, path = get_bucket_and_path_from_gcs_output(output)
        item_exporter = GcsItemExporter(bucket=bucket, path=path)
    elif item_exporter_type == ItemExporterType.PUBSUB:
        from blockchainetl.jobs.exporters.google_pubsub_item_exporter import GooglePubSubItemExporter
        enable_message_ordering = 'sorted' in output or 'ordered' in output
        item_exporter = GooglePubSubItemExporter(
            item_type_to_topic_mapping={
                'checkpoint': output + '.checkpoints',
                'transaction': output + '.transactions',
                'event': output + '.events',
                'object': output + '.objects',
            },
            message_attributes=('item_id', 'item_timestamp'),
            batch_max_bytes=1024 * 1024 * 5,
            batch_max_latency=0.03, # 30ms. Look at metrics to see whether to reduce it further.
            batch_max_messages=1000,
            enable_message_ordering=enable_message_ordering)
    elif item_exporter_type == ItemExporterType.CONSOLE:
        item_exporter = ConsoleItemExporter()
    else:
        raise ValueError("Unable to determine item exporter type for output " + output)

    return item_exporter
