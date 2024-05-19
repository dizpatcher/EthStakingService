"""Модели pydantic - для обозначения типов данных"""

from enum import StrEnum
from pydantic import BaseModel


class Validators(BaseModel):
    class DataItem(BaseModel):
        class StatusType(StrEnum):
            pending_initialized = "pending_initialized"
            pending_queued = "pending_queued"
            active_ongoing = "active_ongoing"
            active_exiting = "active_exiting"
            active_slashed = "active_slashed"
            exited_unslashed = "exited_unslashed"
            exited_slashed = "exited_slashed"
            withdrawal_possible = "withdrawal_possible"
            withdrawal_done = "withdrawal_done"

        class Validator(BaseModel):
            pubkey: str
            effective_balance: int
            slashed: bool

        index: int
        status: StatusType

        validator: Validator

    data: list[DataItem]


class Genesis(BaseModel):
    class Data(BaseModel):
        genesis_time: int

    data: Data


class Header(BaseModel):
    class Data(BaseModel):
        class Header(BaseModel):
            class Message(BaseModel):
                slot: int

            message: Message

        header: Header

    data: Data


class Block(BaseModel):
    class Data(BaseModel):
        class Message(BaseModel):
            class Body(BaseModel):
                class Attestation(BaseModel):
                    class Data(BaseModel):
                        slot: int
                        index: int

                    aggregation_bits: str
                    data: Data

                class ExecutionPayload(BaseModel):
                    fee_recipient: str
                    block_hash: str

                attestations: list[Attestation]
                execution_payload: ExecutionPayload

            slot: int
            proposer_index: int
            body: Body

        message: Message

    data: Data


class Committees(BaseModel):
    class Data(BaseModel):
        index: int
        slot: int
        validators: list[int]

    data: list[Data]


class ProposerDuties(BaseModel):
    class Data(BaseModel):
        pubkey: str
        validator_index: int
        slot: int

    dependent_root: str
    data: list[Data]


class ValidatorsLivenessRequestLighthouse(BaseModel):
    indices: list[int]
    epoch: int


class ValidatorsLivenessRequestTeku(BaseModel):
    indices: list[int]


class ValidatorsLivenessResponse(BaseModel):
    class Data(BaseModel):
        index: int
        is_live: bool

    data: list[Data]


class SlotWithStatus(BaseModel):
    number: int
    missed: bool


class ConsensusClient(StrEnum):
    LIGHTHOUSE = "lighthouse"
    PRYSM = "prysm"
    TEKU = "teku"
    NIMBUS = "nimbus"
    OTHER = "other"


class BlockTerm(StrEnum):
    GENESIS = "genesis"
    FINALIZED = "finalized"
    HEAD = "head"


class EthGetBlockByHashRequest(BaseModel):
    jsonrpc: str = "2.0"
    method: str = "eth_getBlockByHash"
    params: list
    id: str = "1"


class ExecutionBlock(BaseModel):
    """Execution layer блок"""
    
    class Result(BaseModel):
        class Transaction(BaseModel):
            to: str | None

        transactions: list[Transaction]

    jsonrpc: str
    id: int
    result: Result


class Rewards(BaseModel):
    class Data(BaseModel):
        class PossibleReward(BaseModel):
            effective_balance: int
            source: int
            target: int
            head: int

        class EarnedReward(BaseModel):
            validator_index: int
            source: int
            target: int
            head: int

        ideal_rewards: list[PossibleReward]
        total_rewards: list[EarnedReward]

    data: Data
