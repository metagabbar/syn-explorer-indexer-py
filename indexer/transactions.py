from typing import (Optional, Dict, get_args)
from dataclasses import dataclass, fields
from decimal import Decimal
from bson import Decimal128
from attr import field

from hexbytes import HexBytes

from indexer.data import TOKEN_DECIMALS, CHAINS, TOKEN_SYMBOLS
from indexer.helpers import handle_decimals


class Base:
    def __post_init__(self) -> None:
        # Handle token decimals.
        if 'received_token' in self.__dict__:
            chain = CHAINS[self.__dict__['to_chain_id']]
            initial = self.__dict__['received_value']
            token = self.__dict__['received_token']

            if token is not None:
                self.received_token = HexBytes(token)
                decimals = TOKEN_DECIMALS[chain][self.received_token.hex()]
                symbol = TOKEN_SYMBOLS[chain][self.received_token.hex()]

                self.received_token_symbol = symbol
                self.received_value_formatted = handle_decimals(
                    initial,
                    decimals,
                    for_mongo=True
                )
            else:
                self.received_token_symbol = None
                self.received_value_formatted = None

        if 'sent_token' in self.__dict__:
            chain = CHAINS[self.__dict__['from_chain_id']]
            initial = self.__dict__['sent_value']
            token = self.__dict__['sent_token']

            if token is not None:
                self.sent_token = HexBytes(token)
                decimals = TOKEN_DECIMALS[chain][self.sent_token.hex()]
                symbol = TOKEN_SYMBOLS[chain][self.sent_token.hex()]

                self.sent_token_symbol = symbol
                self.sent_value_formatted = handle_decimals(
                    initial,
                    decimals,
                )
            else:
                self.sent_token_symbol = None
                self.sent_value_formatted = None

        for field in fields(self):
            if field.name in [
                    'sent_token_symbol', 'sent_value_formatted',
                    'received_token_symbol', 'received_value_formatted'
            ]:
                continue

            val = self.__dict__[field.name]

            # Pscyopg returns psql's bytea as bytes.
            if type(val) == bytes:
                self.__dict__[field.name] = HexBytes(val)
            # We store ints as varchars in psql due to BIGINT's limitations.
            elif type(val) == str and (field.type == int
                                       or get_args(field.type)[0] == int):
                self.__dict__[field.name] = int(val)
            elif not isinstance(val, get_args(field.type) or field.type):
                raise TypeError(f'expected {field.name!r} to be of type '
                                f'{field.type} not {type(val)}')

    def serialize(self) -> Dict[str, str]:
        res = {}
        for k, v in self.__dict__.items():
            if isinstance(v, Decimal):
                res[k] = Decimal128(v)
            elif isinstance(v, HexBytes):
                res[k] = v.hex()
            else:
                res[k] = v if isinstance(v, str) else str(v)
        return res


@dataclass
class LostTransaction(Base):
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
class Transaction(Base):
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
