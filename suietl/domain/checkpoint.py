from typing import List

class SuiCheckpoint(object):
    def __init__(self):
        self.checkpoint_commitments: List[str] = []
        self.computation_cost: int = None
        self.digest: str = None
        self.epoch: int = None
        self.epoch_commitments: List[str] = None
        self.network_total_transactions: int = None
        self.next_epoch_committee: List[dict] = None
        self.next_epoch_protocol_version: int = None
        self.non_refundable_storage_fee: int = None
        self.previous_digest: str = None
        self.sequence_number: int = None
        self.storage_cost: int = None
        self.storage_rebate: int = None
        self.timestamp_ms: int = None
        self.timestamp: str = None
        self.transactions: List[str] = []
        self.validator_signature: str = None
