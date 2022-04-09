from typing import Dict, Optional, List, Any, Union, Literal
from collections import defaultdict
from dataclasses import dataclass
from hexbytes import HexBytes

import web3.exceptions
from web3 import Web3

_pool_cache: Dict[str, Dict[str, List[str]]] = defaultdict(dict)


@dataclass
class TokenInfo:
    chain_id: int
    address: HexBytes
    decimals: int
    max_swap: int
    min_swap: int
    swap_fee: int
    min_swap_fee: int
    max_swap_fee: int
    has_underlying: bool
    is_underlying: bool


# TODO(blaze): better type hints.
def call_abi(data, key: str, func_name: str, *args, **kwargs) -> Any:
    call_args = kwargs.pop('call_args', {})
    return getattr(data[key].functions, func_name)(*args,
                                                   **kwargs).call(**call_args)


def get_all_tokens_in_pool(chain: str,
                           max_index: Optional[int] = None,
                           func: str = 'nusdpool_contract') -> List[str]:
    """
    Get all tokens by calling `getToken` by iterating from 0 till a
    contract error or `max_index` and implicitly sorted by index.

    Args:
        chain (str): the EVM chain
        max_index (Optional[int], optional): max index to iterate to. 
            Defaults to None.

    Returns:
        List[str]: list of token addresses
    """
    from indexer.data import SYN_DATA, MAX_UINT8

    assert (chain in SYN_DATA)

    data = SYN_DATA[chain]
    res: List[str] = []

    for i in range(max_index or MAX_UINT8):
        try:
            res.append(call_abi(data, func, 'getToken', i))
        except (web3.exceptions.ContractLogicError,
                web3.exceptions.BadFunctionCallOutput):
            # Out of range.
            break

    return res


def get_bridge_token_info(chain_id: int,
                          _id: str) -> Union[Literal[False], TokenInfo]:
    from indexer.data import BRIDGE_CONFIG

    func = BRIDGE_CONFIG.get_function_by_signature('getToken(string,uint256)')
    ret = func(_id, chain_id).call()

    # Does not exist - function's default ret.
    if ret == (0, '0x0000000000000000000000000000000000000000', 0, 0, 0, 0, 0,
               0, False, False):
        return False

    return TokenInfo(*ret)


def bridge_token_to_id(chain_id: int, token: HexBytes) -> str:
    from indexer.data import BRIDGE_CONFIG

    return BRIDGE_CONFIG.functions.getTokenID(token, chain_id).call()


def get_pool_data(chain: str, address: str):
    if address in _pool_cache[chain]:
        return _pool_cache[chain][address]

    from indexer.data import MAX_UINT8, SYN_DATA, BASEPOOL_ABI

    w3: Web3 = SYN_DATA[chain]['w3']
    contract = w3.eth.contract(w3.toChecksumAddress(address), abi=BASEPOOL_ABI)
    res: List[str] = []

    for i in range(MAX_UINT8):
        try:
            # TODO: block identifier?
            res.append(contract.functions.getToken(i).call())
        except (web3.exceptions.ContractLogicError,
                web3.exceptions.BadFunctionCallOutput):
            # Out of range.
            break

    _pool_cache[chain][address] = res
    return res
