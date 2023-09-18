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


def generate_get_checkpoint_by_number_json_rpc(checkpoint_number):
    return generate_json_rpc(
        method="sui_getCheckpoint",
        params=[str(checkpoint_number)],
        request_id=1,
    )


def generate_sui_get_latest_checkpoint_sequence_number():
    return generate_json_rpc(
        method="sui_getLatestCheckpointSequenceNumber",
        params=[],
        request_id=1,
    )


def generate_get_transaction_block_by_number_json_rpc(transaction_hash):
    return generate_json_rpc(
        method="sui_getTransactionBlock",
        params=[
            transaction_hash,
            {
                "showInput": True,
                "showRawInput": False,
                "showEffects": True,
                "showEvents": True,
                "showObjectChanges": True,
                "showBalanceChanges": True,
            },
        ],
        request_id=1,
    )


def generate_json_rpc(method, params, request_id=1):
    return {
        "jsonrpc": "2.0",
        "method": method,
        "params": params,
        "id": request_id,
    }
