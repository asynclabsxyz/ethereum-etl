#  MIT License
#
#  Copyright (c) 2020 Evgeny Medvedev, evge.medvedev@gmail.com
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in all
#  copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#  SOFTWARE.

from blockchainetl.jobs.exporters.console_item_exporter import ConsoleItemExporter
from blockchainetl.jobs.exporters.multi_item_exporter import MultiItemExporter
from ethereumetl.streaming.item_exporter_creator import (
    get_bucket_and_path_from_gcs_output,
    determine_item_exporter_type,
    ItemExporterType,
)


# naman, We need this file because we want to use a modified version of gcs item exporter.
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
        # naman, todo, this needs to be custom too because gcs item exporter has a lot
        # of eth primitives built in
        item_exporter = GcsItemExporter(bucket=bucket, path=path)
    elif item_exporter_type == ItemExporterType.PUBSUB:
        from blockchainetl.jobs.exporters.google_pubsub_item_exporter import GooglePubSubItemExporter
        enable_message_ordering = 'sorted' in output or 'ordered' in output
        item_exporter = GooglePubSubItemExporter(
            item_type_to_topic_mapping={
                'checkpoint': output + '.checkpoints',
                'transaction': output + '.transactions',
            },
            message_attributes=('item_id', 'item_timestamp'),
            batch_max_bytes=1024 * 1024 * 5,
            batch_max_latency=2,
            batch_max_messages=1000,
            enable_message_ordering=enable_message_ordering)
    elif item_exporter_type == ItemExporterType.CONSOLE:
        item_exporter = ConsoleItemExporter()
    else:
        raise ValueError("Unable to determine item exporter type for output " + output)

    return item_exporter
