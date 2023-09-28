from suietl.domain.event import SuiEvent
from suietl.utils import epoch_milliseconds_to_rfc3339
from ethereumetl.utils import to_int_or_none


class SuiEventsMapper(object):
    def json_dict_to_events(self, json_dict):
        checkpoint_number = to_int_or_none(json_dict.get("checkpoint"))
        transaction_digest = json_dict.get("digest")
        timestamp_ms = to_int_or_none(json_dict.get("timestampMs"))
        timestamp = epoch_milliseconds_to_rfc3339(timestamp_ms)

        json_events_dict = json_dict.get("events", [])
        events = []
        for json_event_dict in json_events_dict:
            event = SuiEvent()
            event.checkpoint_sequence_number = checkpoint_number
            event.transaction_digest = transaction_digest
            event.timestamp_ms = timestamp_ms
            event.timestamp = timestamp
            event.event_seq = to_int_or_none(json_event_dict.get("id").get("eventSeq"))
            event.package_id = json_event_dict.get("packageId")
            event.transaction_module = json_event_dict.get("transactionModule")
            event.sender = json_event_dict.get("sender")
            event.event_type = json_event_dict.get("type")
            event.parsed_json = str(json_event_dict.get("parsedJson"))
            event.bcs = json_event_dict.get("bcs")
            events.append(event)

        return events

    def event_to_dict(self, event: SuiEvent):
        return {
            "type": "event",
            "checkpoint_sequence_number": event.checkpoint_sequence_number,
            "transaction_digest": event.transaction_digest,
            "timestamp_ms": event.timestamp_ms,
            "timestamp": event.timestamp,
            "event_seq": event.event_seq,
            "package_id": event.package_id,
            "transaction_module": event.transaction_module,
            "sender": event.sender,
            "event_type": event.event_type,
            "parsed_json": event.parsed_json,
            "bcs": event.bcs,
        }
