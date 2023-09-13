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

        json_transaction_dict = json_dict.get("transaction")
        transaction.tx_signatures = json_transaction_dict.get("txSignatures")

        json_transaction_data_dict = json_transaction_dict.get("data")
        transaction.transaction = json_transaction_data_dict.get("transaction")
        transaction.sender = json_transaction_data_dict.get("sender")
        transaction.gas_data = self.parse_gas_data(
            json_transaction_data_dict.get("gasData")
        )

        return transaction

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
            "owner": to_int_or_none(gas_data.get("storageCost")),
            "price": to_int_or_none(gas_data.get("price")),
            "budget": to_int_or_none(gas_data.get("budget")),
        }

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
        }
