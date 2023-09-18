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


from suietl.domain.transaction_block_event import SuiTransactionBlockEvent
from ethereumetl.utils import to_int_or_none


class SuiTransactionBlockEventsMapper(object):
    def json_dict_to_events(self, json_dict):
        checkpoint_number = to_int_or_none(json_dict.get("checkpoint"))
        transaction_digest = json_dict.get("digest")
        timestamp_ms = to_int_or_none(json_dict.get("timestampMs"))

        json_events_dict = json_dict.get("events", [])
        events = []
        for json_event_dict in json_events_dict:
            event = SuiTransactionBlockEvent()
            event.checkpoint_number = checkpoint_number
            event.transaction_digest = transaction_digest
            event.timestamp_ms = timestamp_ms
            event.id = self.parse_event_id(json_event_dict.get("id"))
            event.package_id = json_event_dict.get("packageId")
            event.transaction_module = json_event_dict.get("transactionModule")
            event.sender = json_event_dict.get("sender")
            event.event_type = json_event_dict.get("type")
            event.parsed_json = str(json_event_dict.get("parsedJson"))
            event.bcs = json_event_dict.get("bcs")
            events.append(event)

        return events

    def parse_event_id(self, event_id):
        return {
            "tx_digest": event_id.get("txDigest"),
            "event_seq": to_int_or_none(event_id.get("eventSeq")),
        }

    # def parse_event_type(self, event_type):
    #     return {
    #         "address": event_type.get("address"),
    #         "module": event_type.get("module"),
    #         "name": event_type.get("name"),
    #         "type_params": event_type.get("typeParams"),
    #     }

    def event_to_dict(self, event):
        return {
            "type": "event",
            "checkpoint_number": event.checkpoint_number,
            "transaction_digest": event.transaction_digest,
            "timestamp_ms": event.timestamp_ms,
            "id": event.id,
            "package_id": event.package_id,
            "transaction_module": event.transaction_module,
            "sender": event.sender,
            "event_type": event.event_type,
            "parsed_json": event.parsed_json,
            "bcs": event.bcs,
        }
