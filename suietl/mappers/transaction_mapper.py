from suietl.domain.transaction import SuiTransaction
from suietl.utils import epoch_milliseconds_to_rfc3339
from ethereumetl.utils import to_int_or_none


class SuiTransactionMapper(object):
    def json_dict_to_transaction(self, json_dict):
        transaction = SuiTransaction()
        transaction.checkpoint_sequence_number = to_int_or_none(json_dict.get("checkpoint"))
        transaction.digest = json_dict.get("digest")
        transaction.timestamp_ms = to_int_or_none(json_dict.get("timestampMs"))
        transaction.timestamp = epoch_milliseconds_to_rfc3339(transaction.timestamp_ms)
        transaction.balance_changes = self.parse_balance_changes(json_dict.get("balanceChanges", []))
        transaction.raw_transaction = json_dict.get("raw_transaction")
        transaction.confirmed_local_execution = json_dict.get("confirmed_local_execution")

        json_effects_dict = json_dict.get("effects")
        transaction.created = self.map_owned_objects(json_effects_dict.get("created", []))
        transaction.mutated = self.map_owned_objects(json_effects_dict.get("mutated", []))
        transaction.unwrapped = self.map_owned_objects(json_effects_dict.get("unwrapped", []))
        transaction.deleted = self.map_unowned_objects(json_effects_dict.get("deleted", []))
        transaction.shared_objects = self.map_unowned_objects(json_effects_dict.get("shared_objects", []))
        transaction.unwrapped_then_deleted = self.map_unowned_objects(json_effects_dict.get("unwrapped_then_deleted", []))
        transaction.wrapped = self.map_unowned_objects(json_effects_dict.get("wrapped", []))
        transaction.execution_status = json_effects_dict.get("status").get("status")
        transaction.executed_epoch = to_int_or_none(json_effects_dict.get("executedEpoch"))
        transaction.events_digest = json_effects_dict.get("eventsDigest")
        transaction.transaction_dependencies = json_effects_dict.get("dependencies")

        gas_used = json_effects_dict.get("gasUsed")
        transaction.computation_cost = gas_used.get("computationCost")
        transaction.storage_cost = gas_used.get("storageCost")
        transaction.storage_rebate = gas_used.get("storageRebate")
        transaction.non_refundable_storage_fee = gas_used.get("nonRefundableStorageFee")
    
        gas_object = json_effects_dict.get("gasObject").get("reference")
        transaction.gas_object_id = gas_object.get("object_id")
        transaction.gas_object_version = to_int_or_none(gas_object.get("version"))
        transaction.gas_object_digest = gas_object.get("digest")
        
        json_transaction_dict = json_dict.get("transaction")
        transaction.tx_signatures = json_transaction_dict.get("txSignatures")

        json_transaction_data_dict = json_transaction_dict.get("data")
        transaction.sender = json_transaction_data_dict.get("sender")
        # There are too may different types of transactions to easily map them
        # https://github.com/MystenLabs/sui/blob/main/crates/sui-json-rpc-types/src/sui_transaction.rs#L277
        transaction.transaction_kind = json_transaction_data_dict.get("transaction").get("kind")
        transaction.transaction_json = str(json_transaction_data_dict.get("transaction"))

        gas_data = json_transaction_data_dict.get("gasData")
        transaction.gas_payments = self.parse_gas_payments(gas_data.get("payment"))
        transaction.gas_price = gas_data.get("price")
        transaction.gas_budget = gas_data.get("budget")

        return transaction
    
    def parse_gas_payments(self, gas_payments):
        return [
            {
                "object_id": payment.get("objectId"),
                "version": to_int_or_none(payment.get("version")),
                "digest": payment.get("digest"),
            } for payment in gas_payments
        ]
    
    def parse_balance_changes(self, balance_changes):
        parsed_balance_changes = []
        for balance_change in balance_changes:
            owner_type, owner_address, initial_shared_version = self.parse_owner(balance_change.get("owner"))
            parsed_balance_change = {
                "amount": balance_change.get("amount"),
                "coin_type": balance_change.get("coinType"),
                "owner_type": owner_type,
                "owner_address": owner_address,
                "initial_shared_version": initial_shared_version
            }
            parsed_balance_changes.append(parsed_balance_change)
        return parsed_balance_changes
    
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
        
    def map_owned_objects(self, owned_objects):
        return [owned_object.get("reference").get("objectId") for owned_object in owned_objects]
    
    def map_unowned_objects(self, unowned_objects):
        return [unowned_object.get("objectId") for unowned_object in unowned_objects]
    
    def transaction_to_dict(self, transaction: SuiTransaction):
        return {
            "type": "transaction",
            "balance_changes": transaction.balance_changes,
            "checkpoint_sequence_number": transaction.checkpoint_sequence_number,
            "created": transaction.created,
            "computation_cost": transaction.computation_cost,
            "confirmed_local_execution": transaction.confirmed_local_execution,
            "deleted": transaction.deleted,
            "digest": transaction.digest,
            "events_digest": transaction.events_digest,
            "executed_epoch": transaction.executed_epoch,
            "execution_status": transaction.execution_status,
            "gas_budget": transaction.gas_budget,
            "gas_object_digest": transaction.gas_object_digest,
            "gas_object_id": transaction.gas_object_id,
            "gas_object_version": transaction.gas_object_version,
            "gas_price": transaction.gas_price,
            "gas_payments": transaction.gas_payments,
            "mutated": transaction.mutated,
            "non_refundable_storage_fee": transaction.non_refundable_storage_fee,
            "raw_transaction": transaction.raw_transaction,
            "sender": transaction.sender,
            "shared_objects": transaction.shared_objects,
            "storage_cost": transaction.storage_cost,
            "storage_rebate": transaction.storage_rebate,
            "timestamp_ms": transaction.timestamp_ms,
            "timestamp": transaction.timestamp,
            "transaction_dependencies": transaction.transaction_dependencies,
            "transaction_json": transaction.transaction_json,
            "transaction_kind": transaction.transaction_kind,
            "tx_signatures": transaction.tx_signatures,
            "unwrapped": transaction.unwrapped,
            "unwrapped_then_deleted": transaction.unwrapped_then_deleted,
            "wrapped": transaction.wrapped,
        }

    # def parse_object_changes(self, object_changes):
    #     parsed_by_type = {}
    #     for object_change in object_changes:
    #         type = object_change.get("type")
    #         if type not in parsed_by_type:
    #             parsed_by_type[type] = []
    #         parsed_object_change = {}
    #         if type == "published":
    #             parsed_object_change = {
    #                 "package_id": object_change.get("packageId"),
    #                 "version": object_change.get("version"),
    #                 "digest": object_change.get("digest"),
    #                 "modules": object_change.get("modules"),
    #             }
    #         elif type == "transferred":
    #             parsed_object_change = {
    #                 "sender": object_change.get("sender"),
    #                 "recipient": self.parse_owner(object_change.get("recipient")),
    #                 "object_type": object_change.get("objectType"),
    #                 "object_id": object_change.get("objectId"),
    #                 "version": object_change.get("version"),
    #                 "digest": object_change.get("digest"),
    #             }
    #         elif type == "mutated":
    #             parsed_object_change = {
    #                 "sender": object_change.get("sender"),
    #                 "owner": self.parse_owner(object_change.get("recipient")),
    #                 "object_type": object_change.get("objectType"),
    #                 "object_id": object_change.get("objectId"),
    #                 "version": object_change.get("version"),
    #                 "previous_version": object_change.get("previousVersion"),
    #                 "digest": object_change.get("digest"),
    #             }
    #         elif type == "deleted":
    #             parsed_object_change = {
    #                 "sender": object_change.get("sender"),
    #                 "object_type": object_change.get("objectType"),
    #                 "object_id": object_change.get("objectId"),
    #                 "version": object_change.get("version"),
    #             }
    #         elif type == "wrapped":
    #             parsed_object_change = {
    #                 "sender": object_change.get("sender"),
    #                 "object_type": object_change.get("objectType"),
    #                 "object_id": object_change.get("objectId"),
    #                 "version": object_change.get("version"),
    #             }
    #         elif type == "created":
    #             parsed_object_change = {
    #                 "sender": object_change.get("sender"),
    #                 "owner": self.parse_owner(object_change.get("recipient")),
    #                 "object_type": object_change.get("objectType"),
    #                 "object_id": object_change.get("objectId"),
    #                 "version": object_change.get("version"),
    #                 "digest": object_change.get("digest"),
    #             }
    #         parsed_by_type[type].append(parsed_object_change)
    #     return parsed_by_type
