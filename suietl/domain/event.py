class SuiEvent(object):
    def __init__(self):
        self.bcs: str = None
        self.checkpoint_sequence_number: int = None
        self.event_type: str = None
        self.event_seq: int = None
        self.package_id: str = None
        self.parsed_json: str = None
        self.sender: str = None
        self.timestamp_ms: int = None
        self.transaction_module: str = None
        self.transaction_digest: str = None
        self.timestamp: str = None
