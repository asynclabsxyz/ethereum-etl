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


from suietl.domain.transaction_block_effects import SuiTransactionBlockEffects
from ethereumetl.utils import to_int_or_none


class SuiTransactionBlockEffectsMapper(object):
    def json_dict_to_effects(self, json_dict):
        effects = SuiTransactionBlockEffects()
        effects.checkpoint_number = to_int_or_none(json_dict.get("checkpoint"))
        effects.transaction_digest = json_dict.get("digest")
        effects.timestamp_ms = to_int_or_none(json_dict.get("timestampMs"))

        json_effects_dict = json_dict.get("effects")
        effects.status = json_effects_dict.get("status")
        effects.executed_epoch = to_int_or_none(json_effects_dict.get("executedEpoch"))
        effects.gas_used = self.parse_gas_used(json_effects_dict.get("gasUsed"))
        effects.modified_at_versions = self.parse_modified_at_versions(
            json_effects_dict.get("modifiedAtVersions", [])
        )
        effects.shared_objects = self.parse_object_refs(
            json_effects_dict.get("sharedObjects", [])
        )
        effects.created = self.parse_owned_objects(json_effects_dict.get("created", []))
        effects.mutated = self.parse_owned_objects(json_effects_dict.get("mutated", []))
        effects.unwrapped = self.parse_owned_objects(
            json_effects_dict.get("unwrapped", [])
        )
        effects.deleted = self.parse_object_refs(json_effects_dict.get("deleted", []))
        effects.unwrapped_then_deleted = self.parse_object_refs(
            json_effects_dict.get("unwrappedThenDeleted", [])
        )
        effects.wrapped = self.parse_object_refs(json_effects_dict.get("wrapped", []))
        effects.gas_object = self.parse_owned_object(json_effects_dict.get("gasObject"))
        effects.events_digest = json_effects_dict.get("eventsDigest", None)
        effects.dependencies = json_effects_dict.get("dependencies")

        return effects

    def parse_gas_used(self, gas_used):
        return {
            "computation_cost": to_int_or_none(gas_used.get("computationCost")),
            "storage_cost": to_int_or_none(gas_used.get("storageCost")),
            "storage_rebate": to_int_or_none(gas_used.get("storageRebate")),
            "non_refundable_storage_fee": to_int_or_none(
                gas_used.get("nonRefundableStorageFee")
            ),
        }

    def parse_modified_at_versions(self, modified_at_versions):
        return [
            {
                "object_id": version.get("objectId"),
                "sequence_number": version.get("sequenceNumber"),
            }
            for version in modified_at_versions
        ]

    def parse_object_refs(self, shared_objects):
        return [
            {
                "object_id": shared_object.get("objectId"),
                "sequence_number": shared_object.get("sequenceNumber"),
                "digest": shared_object.get("digest"),
            }
            for shared_object in shared_objects
        ]

    def parse_owned_objects(self, owned_objects):
        return [self.parse_owned_object(owned_object) for owned_object in owned_objects]

    def parse_owned_object(self, owned_object):
        return {
            # naman, there are too may different types to easily map them
            # https://github.com/MystenLabs/sui/blob/da0d7881e366f78abb43b7b27661b50c8083c079/sdk/typescript/src/types/common.ts#L38
            "owner": owned_object.get("owner"),
            "reference": {
                "object_id": owned_object.get("reference").get("objectId"),
                "version": owned_object.get("reference").get("version"),
                "digest": owned_object.get("reference").get("digest"),
            },
        }

    def effects_to_dict(self, effects):
        return {
            "type": "effect",
            "checkpoint_number": effects.checkpoint_number,
            "transaction_digest": effects.transaction_digest,
            "timestamp_ms": effects.timestamp_ms,
            "status": effects.status,
            "executed_epoch": effects.executed_epoch,
            "gas_used": effects.gas_used,
            "modified_at_versions": effects.modified_at_versions,
            "shared_objects": effects.shared_objects,
            "created": effects.created,
            "mutated": effects.mutated,
            "unwrapped": effects.unwrapped,
            "deleted": effects.deleted,
            "unwrapped_then_deleted": effects.unwrapped_then_deleted,
            "wrapped": effects.wrapped,
            "gas_object": effects.gas_object,
            "events_digest": effects.events_digest,
            "dependencies": effects.dependencies,
        }
