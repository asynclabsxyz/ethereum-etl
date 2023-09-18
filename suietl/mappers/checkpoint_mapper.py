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


from suietl.domain.checkpoint import SuiCheckpoint
from ethereumetl.utils import to_int_or_none


# naman: see rust sdk to get the object structure
# https://github.com/MystenLabs/sui/blob/main/crates/sui-json-rpc-types/src/sui_checkpoint.rs


# TODO, naman: I don't know if we need to normalize addresses in SUI
class SuiCheckpointMapper(object):
    def json_dict_to_block(self, json_dict):
        checkpoint = SuiCheckpoint()

        checkpoint.checkpoint_commitments = self.parse_checkpoint_commitments(json_dict.get("checkpointCommitments", []))
        checkpoint.end_of_epoch_data = self.parse_end_of_epoch_data(json_dict.get("endOfEpochData"))
        checkpoint.digest = json_dict.get("digest")
        checkpoint.epoch = to_int_or_none(json_dict.get("epoch"))
        checkpoint.epoch_rolling_gas_cost_summary = self.parse_gas_cost_summary(
            json_dict.get("epochRollingGasCostSummary")
        )
        checkpoint.network_total_transactions = to_int_or_none(
            json_dict.get("networkTotalTransactions")
        )
        checkpoint.previous_digest = json_dict.get("previousDigest")
        checkpoint.sequence_number = to_int_or_none(json_dict.get("sequenceNumber"))
        checkpoint.timestamp_ms = to_int_or_none(json_dict.get("timestampMs"))
        checkpoint.transactions = json_dict.get("transactions")
        checkpoint.validator_signature = json_dict.get("validatorSignature")

        return checkpoint

    def parse_checkpoint_commitments(self, checkpointCommitments):
        return [
            {
                "digest": checkpointCommitment.get("digest"),
            } for checkpointCommitment in checkpointCommitments
        ]
    
    def parse_end_of_epoch_data(self, endOfEpochData):
        if endOfEpochData is None:
            return None
        return {
            "next_epoch_committee": self.parse_committee_members(endOfEpochData.get("nextEpochCommittee", [])),
            "next_epoch_protocol_version": to_int_or_none(endOfEpochData.get("nextEpochProtocolVersion")),
            "epoch_commitments": self.parse_checkpoint_commitments(endOfEpochData.get("epochCommitments", []))
        }
    
    def parse_committee_members(self, committeeMembers):
        return [
            {
                "authority_name": committeeMember[0],
                "stake_unit": committeeMember[1],
            } for committeeMember in committeeMembers
        ]

    def parse_gas_cost_summary(self, summary):
        return {
            "computation_cost": to_int_or_none(summary.get("computationCost")),
            "storage_cost": to_int_or_none(summary.get("storageCost")),
            "storage_rebate": to_int_or_none(summary.get("storageRebate")),
            "non_refundable_storage_fee": to_int_or_none(
                summary.get("nonRefunableStorageFee")
            ),
        }

    def checkpoint_to_dict(self, checkpoint):
        return {
            "type": "checkpoint",
            "checkpoint_commitments": checkpoint.checkpoint_commitments,
            "end_of_epoch_data": checkpoint.end_of_epoch_data,
            "digest": checkpoint.digest,
            "epoch": checkpoint.epoch,
            "epoch_rolling_gas_cost_summary": checkpoint.epoch_rolling_gas_cost_summary,
            "network_total_transactions": checkpoint.network_total_transactions,
            "previous_digest": checkpoint.previous_digest,
            "sequence_number": checkpoint.sequence_number,
            "timestamp_ms": checkpoint.timestamp_ms,
            "transactions": checkpoint.transactions,
            "validator_signature": checkpoint.validator_signature,
        }
