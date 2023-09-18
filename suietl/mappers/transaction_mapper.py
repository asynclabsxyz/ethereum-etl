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


from suietl.domain.transaction import SuiTransaction
from ethereumetl.utils import to_int_or_none


class SuiTransactionMapper(object):
    def json_dict_to_transaction(self, json_dict):
        transaction = SuiTransaction()
        transaction.checkpoint_number = to_int_or_none(json_dict.get("checkpoint"))
        transaction.digest = json_dict.get("digest")
        transaction.timestamp_ms = to_int_or_none(json_dict.get("timestampMs"))
        transaction.balance_changes = self.parse_balance_changes(json_dict.get("balanceChanges", []))
        transaction.object_changes = self.parse_object_changes(json_dict.get("objectChanges", []))

        json_transaction_dict = json_dict.get("transaction")
        transaction.tx_signatures = json_transaction_dict.get("txSignatures")

        json_transaction_data_dict = json_transaction_dict.get("data")
        # naman, there are too may different types of transactions to easily map them
        # https://github.com/MystenLabs/sui/blob/main/crates/sui-json-rpc-types/src/sui_transaction.rs#L277
        transaction.transaction = str(json_transaction_data_dict.get("transaction"))
        transaction.sender = json_transaction_data_dict.get("sender")
        transaction.gas_data = self.parse_gas_data(
            json_transaction_data_dict.get("gasData")
        )

        return transaction
    
    def parse_balance_changes(self, balance_changes):
        return [
            {
                "owner": self.parse_owner(balance_change.get("owner")),
                "coin_type": balance_change.get("coinType"),
                "amount": balance_change.get("amount"),
            } for balance_change in balance_changes
        ]
    
    # https://github.com/MystenLabs/sui/blob/main/crates/sui-types/src/object.rs#L533
    def parse_owner(self, owner):
        if owner is None:
            return None
        if isinstance(owner, str):
            if owner == "Immutable":
                return {
                    "immutable": "immutable"
                }
            return None
        return {
            "address_owner": owner.get("AddressOwner"),
            "object_owner": owner.get("ObjectOwner"),
            "shared": owner.get("Shared"),
        }
    
    def parse_object_changes(self, object_changes):
        parsed_by_type = {}
        for object_change in object_changes:
            type = object_change.get("type")
            if type not in parsed_by_type:
                parsed_by_type[type] = []
            parsed_object_change = {}
            if type == "published":
                parsed_object_change = {
                    "package_id": object_change.get("packageId"),
                    "version": object_change.get("version"),
                    "digest": object_change.get("digest"),
                    "modules": object_change.get("modules"),
                }
            elif type == "transferred":
                parsed_object_change = {
                    "sender": object_change.get("sender"),
                    "recipient": self.parse_owner(object_change.get("recipient")),
                    "object_type": object_change.get("objectType"),
                    "object_id": object_change.get("objectId"),
                    "version": object_change.get("version"),
                    "digest": object_change.get("digest"),
                }
            elif type == "mutated":
                parsed_object_change = {
                    "sender": object_change.get("sender"),
                    "owner": self.parse_owner(object_change.get("recipient")),
                    "object_type": object_change.get("objectType"),
                    "object_id": object_change.get("objectId"),
                    "version": object_change.get("version"),
                    "previous_version": object_change.get("previousVersion"),
                    "digest": object_change.get("digest"),
                }
            elif type == "deleted":
                parsed_object_change = {
                    "sender": object_change.get("sender"),
                    "object_type": object_change.get("objectType"),
                    "object_id": object_change.get("objectId"),
                    "version": object_change.get("version"),
                }
            elif type == "wrapped":
                parsed_object_change = {
                    "sender": object_change.get("sender"),
                    "object_type": object_change.get("objectType"),
                    "object_id": object_change.get("objectId"),
                    "version": object_change.get("version"),
                }
            elif type == "created":
                parsed_object_change = {
                    "sender": object_change.get("sender"),
                    "owner": self.parse_owner(object_change.get("recipient")),
                    "object_type": object_change.get("objectType"),
                    "object_id": object_change.get("objectId"),
                    "version": object_change.get("version"),
                    "digest": object_change.get("digest"),
                }
            parsed_by_type[type].append(parsed_object_change)
        return parsed_by_type

    def parse_gas_data(self, gas_data):
        payments = gas_data.get("payment")
        return {
            "payment": [
                {
                    "object_id": payment.get("objectId"),
                    "version": to_int_or_none(payment.get("version")),
                    "digest": payment.get("digest"),
                }
                for payment in payments
            ],
            "owner": gas_data.get("owner"),
            "price": to_int_or_none(gas_data.get("price")),
            "budget": to_int_or_none(gas_data.get("budget")),
        }
    
    def parse_transaction(self, transaction):
        return {}

    def transaction_to_dict(self, transaction):
        return {
            "type": "transaction",
            "checkpoint_number": transaction.checkpoint_number,
            "digest": transaction.digest,
            "sender": transaction.sender,
            "gas_data": transaction.gas_data,
            "timestamp_ms": transaction.timestamp_ms,
            "transaction": transaction.transaction,
            "tx_signatures": transaction.tx_signatures,
            "balance_changes": transaction.balance_changes,
            "object_changes": transaction.object_changes,
        }
