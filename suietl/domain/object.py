class SuiObject(object):
    def __init__(self):
        self.checkpoint_sequence_number: str = None
        self.object_digest: str = None
        self.object_id: str = None
        self.version: int = None
        self.owner_type: str = None
        self.owner_address: str = None
        self.initial_shared_version: int = None
        self.previous_transaction: str = None
        self.object_type: str = None
        self.object_status: str = None
        self.timestamp_ms: int = None
        self.timestamp: str = None