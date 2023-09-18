class EntityType:
    CHECKPOINT = "checkpoint"
    EVENT = "event"
    EFFECT = "effect"
    TRANSACTION = "transaction"

    ALL_FOR_STREAMING = [
        CHECKPOINT,
        EVENT,
        EFFECT,
        TRANSACTION,
    ]
