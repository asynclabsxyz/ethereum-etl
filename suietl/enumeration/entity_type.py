class EntityType:
    CHECKPOINT = "checkpoint"
    EVENT = "event"
    OBJECT = "object"
    TRANSACTION = "transaction"

    ALL_FOR_STREAMING = [
        CHECKPOINT,
        EVENT,
        OBJECT,
        TRANSACTION,
    ]
