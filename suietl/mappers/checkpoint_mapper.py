from suietl.domain.checkpoint import SuiCheckpoint
from suietl.utils import epoch_milliseconds_to_rfc3339
from ethereumetl.utils import to_int_or_none


# https://github.com/MystenLabs/sui/blob/main/crates/sui-json-rpc-types/src/sui_checkpoint.rs
# TODO, naman: I don't know if we need to normalize addresses in SUI
class SuiCheckpointMapper(object):
    def json_dict_to_block(self, json_dict):
        checkpoint = SuiCheckpoint()

        checkpoint.checkpoint_commitments = self.parse_checkpoint_commitments(json_dict.get("checkpointCommitments", []))
        checkpoint.digest = json_dict.get("digest")
        checkpoint.epoch = to_int_or_none(json_dict.get("epoch"))
        checkpoint.previous_digest = json_dict.get("previousDigest")
        checkpoint.sequence_number = to_int_or_none(json_dict.get("sequenceNumber"))
        checkpoint.timestamp_ms = to_int_or_none(json_dict.get("timestampMs"))
        checkpoint.timestamp = epoch_milliseconds_to_rfc3339(checkpoint.timestamp_ms)
        checkpoint.transactions = json_dict.get("transactions")
        checkpoint.validator_signature = json_dict.get("validatorSignature")
        checkpoint.network_total_transactions = to_int_or_none(
            json_dict.get("networkTotalTransactions")
        )

        endOfEpochData = json_dict.get("endOfEpochData")
        if endOfEpochData is not None:
            checkpoint.next_epoch_committee = self.parse_committee_members(endOfEpochData.get("nextEpochCommittee", []))
            checkpoint.next_epoch_protocol_version = to_int_or_none(endOfEpochData.get("nextEpochProtocolVersion"))
            checkpoint.epoch_commitments = self.parse_checkpoint_commitments(endOfEpochData.get("epochCommitments", []))
        
        summary = json_dict.get("epochRollingGasCostSummary")
        checkpoint.computation_cost: summary.get("computationCost")
        checkpoint.storage_cost: summary.get("storageCost")
        checkpoint.storage_rebate: summary.get("storageRebate")
        checkpoint.non_refundable_storage_fee: summary.get("nonRefunableStorageFee")
        
        return checkpoint

    def parse_checkpoint_commitments(self, checkpointCommitments):
        return [
            checkpointCommitment.get("digest") for checkpointCommitment in checkpointCommitments
        ]
    
    def parse_committee_members(self, committeeMembers):
        return [
            {
                "authority_name": committeeMember[0],
                "stake_unit": committeeMember[1],
            } for committeeMember in committeeMembers
        ]

    def checkpoint_to_dict(self, checkpoint: SuiCheckpoint):
        return {
            "type": "checkpoint",
            "checkpoint_commitments": checkpoint.checkpoint_commitments,
            "computation_cost": checkpoint.computation_cost,
            "digest": checkpoint.digest,
            "epoch": checkpoint.epoch,
            "epoch_commitments": checkpoint.epoch_commitments,
            "network_total_transactions": checkpoint.network_total_transactions,
            "next_epoch_committee": checkpoint.next_epoch_committee,
            "next_epoch_protocol_version": checkpoint.next_epoch_protocol_version,
            "non_refundable_storage_fee": checkpoint.non_refundable_storage_fee,
            "previous_digest": checkpoint.previous_digest,
            "sequence_number": checkpoint.sequence_number,
            "storage_cost": checkpoint.storage_cost,
            "storage_rebate": checkpoint.storage_rebate,
            "timestamp_ms": checkpoint.timestamp_ms,
            "timestamp": checkpoint.timestamp,
            "transactions": checkpoint.transactions,
            "validator_signature": checkpoint.validator_signature,
        }
