from typing import List, Dict, Optional, Tuple, TypeVar, Union, cast, Literal, Any, Callable
from contextlib import suppress
import traceback
import decimal
import logging
from bson.decimal128 import Decimal128

from web3.types import TxReceipt, LogReceipt
from web3.exceptions import MismatchedABI
from hexbytes import HexBytes
from gevent import Greenlet
import gevent

from indexer.contract import get_bridge_token_info, bridge_token_to_id
from indexer.data import SYN_DATA, POOLS, TOKENS_INFO, CHAINS_REVERSED

logger = logging.Logger(__name__)
D = decimal.Decimal
KT = TypeVar('KT')
VT = TypeVar('VT')
T = TypeVar('T')


def convert(value: T) -> Union[T, str, List]:
    if isinstance(value, HexBytes):
        return value.hex()
    elif isinstance(value, list):
        return [convert(item) for item in value]
    else:
        return value


def is_in_range(value: int, min: int, max: int) -> bool:
    return min <= value <= max


def get_airdrop_value_for_block(ranges: Dict[float, List[Optional[int]]],
                                block: int) -> D:
    def _transform(num: float) -> D:
        return D(str(num))

    for airdrop, _ranges in ranges.items():
        # `_ranges` should have a [0] (start) and a [1] (end)
        assert len(_ranges) == 2, f'expected {_ranges} to have 2 items'

        _min: int
        _max: int

        # Has always been this airdrop value.
        if _ranges[0] is None and _ranges[1] is None:
            return _transform(airdrop)
        elif _ranges[0] is None:
            _min = 0
            _max = cast(int, _ranges[1])

            if is_in_range(block, _min, _max):
                return _transform(airdrop)
        elif _ranges[1] is None:
            _min = _ranges[0]

            if _min <= block:
                return _transform(airdrop)
        else:
            _min, _max = cast(List[int], _ranges)

            if is_in_range(block, _min, _max):
                return _transform(airdrop)

    raise RuntimeError('did not converge', block, ranges)


def address_to_pool(chain: str, address: str) -> Literal['nusd', 'neth']:
    for k, v in POOLS[chain].items():
        if v.lower() == address.lower():
            return k

    raise RuntimeError(f"{address} not found in {chain}'s pools")


def search_logs(chain: str, receipt: TxReceipt,
                received_token: HexBytes) -> Dict[str, Any]:
    contract = TOKENS_INFO[chain][received_token.hex()]['_contract'].events

    for log in receipt['logs']:
        if log['address'].lower() == received_token.hex():
            with suppress(MismatchedABI):
                return contract.Transfer().processLog(log)['args']

    raise RuntimeError(
        f'did not converge: {chain}\n{received_token.hex()}\n{receipt}')


def iterate_receipt_logs(receipt: TxReceipt,
                         check: Callable[[HexBytes, int], bool],
                         check_reverse: bool = True) -> Tuple[HexBytes, int]:
    logs = reversed(receipt['logs']) if check_reverse else receipt['logs']

    for log in logs:
        received = int(log['data'], 16)
        token = HexBytes(log['address'])

        if check(token, received):
            return token, received

    raise RuntimeError(f'did not converge {receipt}')


def dispatch_get_logs(
        cb: Callable[[str, str, LogReceipt], None],
        topics: List[str] = None,
        key_namespace: str = 'logs',
        address_key: str = 'bridge',
        join_all: bool = True,
) -> Optional[List[Greenlet]]:
    """
    dispatch_get_logs polls forwards from the latest block in redis
    (MAX_BLOCK_STORED) until the current block effectively backfilling data
    """

    from indexer.rpc import get_logs

    jobs: List[Greenlet] = []

    for chain in SYN_DATA:
        address = SYN_DATA[chain][address_key]

        if chain in [
            'harmony',
            'ethereum',
            'moonriver',
            'moonbeam',
        ]:
            jobs.append(
                gevent.spawn(get_logs,
                             chain,
                             cb,
                             address,
                             max_blocks=1024,
                             key_namespace=key_namespace))
        elif chain == 'cronos':
            jobs.append(
                gevent.spawn(get_logs,
                             chain,
                             cb,
                             address,
                             max_blocks=2000,
                             key_namespace=key_namespace))
        elif chain in ['boba', 'bsc']:
            jobs.append(
                gevent.spawn(get_logs,
                             chain,
                             cb,
                             address,
                             max_blocks=512,
                             key_namespace=key_namespace))
        else:
            jobs.append(
                gevent.spawn(get_logs,
                             chain,
                             cb,
                             address,
                             key_namespace=key_namespace))

    if join_all:
        gevent.joinall(jobs)
    else:
        return jobs


def retry(func: Callable[..., T], *args, **kwargs) -> Optional[T]:
    attempts: int = kwargs.pop('attempts', 5)

    for i in range(attempts):
        try:
            return func(*args, **kwargs)
        except Exception:
            print(f'retry attempt {i}, args: {args}')
            traceback.print_exc()
            gevent.sleep(3 ** i)

    logging.critical(f'maximum retries ({attempts}) reached')


def token_address_to_pool(chain: str, address: str) -> Literal['neth', 'nusd']:
    for token, v in TOKENS_INFO[chain].items():
        if token == address.lower():
            if (v['symbol'] == 'nETH'
                    or chain == 'ethereum' and v['symbol'] == 'WETH'):
                return 'neth'
            elif v['symbol'] == 'nUSD':
                return 'nusd'

    raise RuntimeError(f'{address} on {chain} did not converge')


def find_same_token_across_chain(chain: str, to_chain: str,
                                 token: HexBytes) -> HexBytes:
    from_chain_id = CHAINS_REVERSED[chain]
    to_chain_id = CHAINS_REVERSED[to_chain]

    symbol = bridge_token_to_id(from_chain_id, token)
    if (data := get_bridge_token_info(to_chain_id, symbol)):
        return data.address

    raise RuntimeError(f'{token} on {chain} to {to_chain} did not converge')


def handle_decimals(num: Union[str, int, float, D], decimals: int) -> D:
    if type(num) != D:
        num = str(num)

    return D(num) / 10 ** decimals
