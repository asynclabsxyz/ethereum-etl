class EntityType:
    CHECKPOINT = "checkpoint"
    EVENT = "event"
    EFFECTS = "effects"
    TRANSACTION = "transaction"

    ALL_FOR_STREAMING = [
        CHECKPOINT,
        EVENT,
        EFFECTS,
        TRANSACTION,
    ]
