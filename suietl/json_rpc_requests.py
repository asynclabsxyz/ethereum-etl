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
