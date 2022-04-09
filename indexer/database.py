from typing import (Any, List, Literal, Tuple, Generator, overload, Optional,
                    get_args)
from dataclasses import dataclass, fields
from decimal import Decimal
from attr import field

from hexbytes import HexBytes


@dataclass
class LostTransaction:
    to_tx_hash: HexBytes
    to_address: HexBytes
    received_value: int
    to_chain_id: int
    received_time: int
    received_token: HexBytes
    swap_success: Optional[bool]
    kappa: HexBytes
    received_value_formatted: Decimal = field(init=False)
    received_token_symbol: str = field(init=False)


@dataclass
class Transaction:
    from_tx_hash: HexBytes
    to_tx_hash: Optional[HexBytes]
    from_address: HexBytes
    to_address: HexBytes
    sent_value: int
    received_value: Optional[int]
    pending: bool
    from_chain_id: int
    to_chain_id: int
    sent_time: int
    received_time: Optional[int]
    received_token: Optional[HexBytes]
    sent_token: HexBytes
    swap_success: Optional[bool]
    kappa: HexBytes
    received_value_formatted: Optional[Decimal] = field(init=False)
    received_token_symbol: Optional[str] = field(init=False)
    sent_value_formatted: Decimal = field(init=False)
    sent_token_symbol: str = field(init=False)
