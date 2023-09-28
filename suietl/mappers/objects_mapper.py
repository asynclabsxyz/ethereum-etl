from typing import List

from suietl.domain.object import SuiObject
from suietl.utils import epoch_milliseconds_to_rfc3339
from ethereumetl.utils import to_int_or_none


class SuiObjectsMapper(object):
    def json_dict_to_objects(self, json_dict):
        json_effects_dict = json_dict.get("effects")

        checkpoint_number = to_int_or_none(json_dict.get("checkpoint"))
        transaction_digest = json_dict.get("digest")
        timestamp_ms = to_int_or_none(json_dict.get("timestampMs"))
        transaction_timestamp = epoch_milliseconds_to_rfc3339(timestamp_ms)
        object_type_by_id = self.map_object_types(json_dict.get("objectChanges", []))

        objects : List[SuiObject] = []
        objects.extend(self.map_owned_objects(
            owned_objects = json_effects_dict.get("created", []), 
            object_status = "created", 
            object_type_by_id = object_type_by_id
        ))
        objects.extend(self.map_owned_objects(
            owned_objects = json_effects_dict.get("mutated", []), 
            object_status = "mutated", 
            object_type_by_id = object_type_by_id
        ))
        objects.extend(self.map_owned_objects(
            owned_objects = json_effects_dict.get("unwrapped", []), 
            object_status = "unwrapped", 
            object_type_by_id = object_type_by_id
        ))
        objects.extend(self.map_unowned_objects(
            unowned_objects = json_effects_dict.get("deleted", []), 
            object_status = "deleted", 
            object_type_by_id = object_type_by_id
        ))
        objects.extend(self.map_unowned_objects(
            unowned_objects = json_effects_dict.get("unwrapped_then_deleted", []), 
            object_status = "unwrapped_then_deleted", 
            object_type_by_id = object_type_by_id
        ))
        objects.extend(self.map_unowned_objects(
            unowned_objects = json_effects_dict.get("wrapped", []), 
            object_status = "wrapped", 
            object_type_by_id = object_type_by_id
        ))

        for object in objects:
            object.checkpoint_sequence_number = checkpoint_number
            object.previous_transaction = transaction_digest
            object.timestamp_ms = timestamp_ms
            object.timestamp = transaction_timestamp

        return objects
    
    def map_object_types(self, object_changes):
        object_type_by_id = {}
        for object_change in object_changes:
            if "objectId" in object_change: 
                object_type_by_id[object_change.get("objectId")] = object_change.get("objectType")
        return object_type_by_id
    
    def map_owned_objects(self, owned_objects, object_status, object_type_by_id):
        objects = []
        for owned_object in owned_objects:
            object = SuiObject()
            object.object_status = object_status
            object.owner_type, object.owner_address, object.initial_shared_version = self.parse_owner(owned_object.get("owner"))
            object.version = to_int_or_none(owned_object.get("reference").get("version"))
            object.object_digest = owned_object.get("reference").get("digest")
            object.object_id = owned_object.get("reference").get("objectId")
            object.object_type = object_type_by_id.get(object.object_id, "")
            objects.append(object)
        return objects
    
    def map_unowned_objects(self, unowned_objects, object_status, object_type_by_id):
        objects = []
        for unowned_object in unowned_objects:
            object = SuiObject()
            object.object_status = object_status
            object.version = to_int_or_none(unowned_object.get("version"))
            object.object_digest = unowned_object.get("digest")
            object.object_id = unowned_object.get("objectId")
            object.object_type = object_type_by_id.get(object.object_id, "")
            objects.append(object)
        return objects
    
    # https://github.com/MystenLabs/sui/blob/b6ecfa0c2d0efcb8ac58d0ab0710a37b6b4e4dcd/crates/sui-types/src/object.rs#L553
    def parse_owner(self, owner):
        if owner is None:
            return None, None, None
        if isinstance(owner, str):
            if owner == "Immutable":
                return "immutable", None, None
            return None, None, None
        if owner.get("AddressOwner") is not None:
            return "address_owner", owner.get("AddressOwner"), None
        if owner.get("ObjectOwner") is not None:
            return "object_owner", owner.get("ObjectOwner"), None
        if owner.get("Shared") is not None:
            return "shared", None, owner.get("Shared").get("initial_shared_version")

    def object_to_dict(self, object: SuiObject):
        return {
            "type": "object",
            "checkpoint_sequence_number": object.checkpoint_sequence_number,
            "object_digest": object.object_digest,
            "object_id": object.object_id,
            "version": object.version,
            "owner_type": object.owner_type,
            "owner_address": object.owner_address,
            "initial_shared_version": object.initial_shared_version,
            "previous_transaction": object.previous_transaction,
            "object_type": object.object_type,
            "object_status": object.object_status,
            "timestamp_ms": object.timestamp_ms,
            "timestamp": object.timestamp,
        }
