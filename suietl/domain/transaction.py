from typing import List

class SuiTransaction(object):
    def __init__(self):
        self.balance_changes: List[dict] = []
        self.checkpoint_sequence_number: int = None
        self.created: List[str] = None
        self.computation_cost: int = None
        self.confirmed_local_execution: bool = None
        self.deleted: List[str] = None
        self.digest: str = None
        self.events_digest: str = None
        self.executed_epoch: int = None
        self.execution_status: str = None
        self.gas_budget: int = None
        self.gas_object_digest: str = None
        self.gas_object_id: str = None
        self.gas_object_version: int = None
        self.gas_price: int = None
        self.gas_payments: List[dict] = None
        self.mutated: List[str] = None
        self.non_refundable_storage_fee: int = None
        self.raw_transaction: str = None
        self.sender: str = None
        self.shared_objects: List[str] = None
        self.storage_cost: int = None
        self.storage_rebate: int = None
        self.timestamp_ms: int = None
        self.timestamp: str = None
        self.transaction_dependencies: List[str] = None
        self.transaction_json: str = None
        self.transaction_kind: str = None
        self.tx_signatures: List[str] = []
        self.unwrapped: List[str] = None
        self.unwrapped_then_deleted: List[str] = None
        self.wrapped: List[str] = None
