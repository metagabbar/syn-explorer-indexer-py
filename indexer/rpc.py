from typing import Callable, Dict, Optional, Tuple, Union, cast, List, overload
from collections import namedtuple
import time
from indexer.db import MongoManager
from pymongo.database import Database
from web3.types import FilterParams, LogReceipt
from hexbytes import HexBytes
from web3 import Web3
import gevent

from indexer.data import BRIDGE_ABI, SYN_DATA, LOGS_REDIS_URL, \
    TOKENS_INFO, TOPICS, TOPIC_TO_EVENT, Direction, CHAINS_REVERSED, \
    MISREPRESENTED_MAP
from indexer.helpers import convert, retry, search_logs, \
    iterate_receipt_logs
from indexer.transactions import Transaction, LostTransaction
from indexer.contract import get_pool_data

# Start blocks of the 4pool >=Nov-7th-2021.
_start_blocks = {
    'ethereum': 13566427,
    'arbitrum': 2876718,  # nUSD pool
    'avalanche': 6619002,  # nUSD pool
    'bsc': 12431591,  # nUSD pool
    'fantom': 21297076,  # nUSD Pool
    'polygon': 21071348,  # nUSD pool
    'harmony': 19163634,  # nUSD pool
    'boba': 16221,  # nUSD pool
    'moonriver': 890949,
    'optimism': 30819,  # nETH pool
    'aurora': 56092179,
    'moonbeam': 173355,
    'cronos': 1578335,
    'metis': 957508,
    'dfk': 0,
}

WETH = HexBytes('0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2')
MAX_BLOCKS = 2048

OUT_SQL = """
INSERT into
    txs (
        from_tx_hash,
        from_address,
        to_address,
        sent_value,
        from_chain_id,
        to_chain_id,
        sent_time,
        sent_token,
        kappa
    )
VALUES
    (%s, %s, %s, %s, %s, %s, %s, %s, %s);
"""

IN_SQL = """
UPDATE
    txs
SET
    (
        to_tx_hash,
        received_value,
        pending,
        received_time,
        received_token,
        swap_success
    ) = (
        %s,
        %s,
        false,
        %s,
        %s,
        %s
    )
WHERE
    kappa = %s;
"""

LOST_IN_SQL = """
INSERT into
    lost_txs (
        to_tx_hash,
        to_address,
        received_value,
        to_chain_id,
        received_time,
        received_token,
        swap_success,
        kappa
    )
VALUES
    (%s, %s, %s, %s, %s, %s, %s, %s);
"""


class Events(object):
    # OUT EVENTS
    @classmethod
    def TokenDepositAndSwap(cls, args):
        x = namedtuple('x', ['to', 'chain_id', 'token_idx_to'])
        return x(HexBytes(args['to']), args['chainId'], args['tokenIndexTo'])

    TokenRedeemAndSwap = TokenDepositAndSwap

    @classmethod
    def TokenDeposit(cls, args):
        x = namedtuple('x', ['to', 'chain_id', 'sent_token', 'sent_value'])
        return x(HexBytes(args['to']), args['chainId'], args['token'],
                 args['amount'])

    @classmethod
    def TokenRedeemAndRemove(cls, args):
        x = namedtuple('x', ['to', 'chain_id', 'token_idx_to'])
        return x(HexBytes(args['to']), args['chainId'], args['swapTokenIndex'])

    @classmethod
    def TokenRedeem(cls, args):
        x = namedtuple('x', ['to', 'chain_id', 'token'])
        return x(HexBytes(args['to']), args['chainId'], args['token'])

    # IN EVENTS
    @classmethod
    def TokenWithdrawAndRemove(cls, args):
        x = namedtuple('x',
                       ['to', 'fee', 'token_idx_to', 'swap_success', 'token'])
        return x(HexBytes(args['to']), args['fee'], args['swapTokenIndex'],
                 args['swapSuccess'], args['token'])

    @classmethod
    def TokenWithdraw(cls, args):
        x = namedtuple('x', ['to', 'fee', 'token', 'amount'])
        return x(HexBytes(args['to']), args['fee'], args['token'],
                 args['amount'])

    TokenMint = TokenWithdraw

    @classmethod
    def TokenMintAndSwap(cls, args):
        x = namedtuple('x',
                       ['to', 'fee', 'token_idx_to', 'swap_success', 'token'])
        return x(HexBytes(args['to']), args['fee'], args['tokenIndexTo'],
                 args['swapSuccess'], args['token'])


def check_factory(max_value: int):
    def check(token: HexBytes, received: int) -> bool:
        return max_value >= received

    return check


@overload
def bridge_callback(chain: str,
                    address: str,
                    log: LogReceipt,
                    abi: str = BRIDGE_ABI,
                    save_block_index: bool = True) -> None:
    ...


@overload
def bridge_callback(
        chain: str,
        address: str,
        log: LogReceipt,
        abi: str = BRIDGE_ABI,
        save_block_index: bool = True,
        testing: bool = False) -> Union[Transaction, LostTransaction]:
    ...


# REF: https://github.com/synapsecns/synapse-contracts/blob/master/contracts/bridge/SynapseBridge.sol#L63-L129
def bridge_callback(
        chain: str,
        address: str,
        log: LogReceipt,
        abi: str = BRIDGE_ABI,
        save_block_index: bool = True,
        testing: bool = False
) -> Optional[Union[Transaction, LostTransaction]]:
    w3: Web3 = SYN_DATA[chain]['w3']
    contract = w3.eth.contract(w3.toChecksumAddress(address), abi=abi)
    tx_hash = log['transactionHash']

    timestamp = w3.eth.get_block(log['blockNumber'])
    timestamp = timestamp['timestamp']  # type: ignore
    tx_info = w3.eth.get_transaction(tx_hash)
    assert 'from' in tx_info  # Make mypy happy - look key 'from' exists!
    from_chain = CHAINS_REVERSED[chain]

    # The info before wrapping the asset can be found in the receipt.
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash,
                                                  timeout=10,
                                                  poll_latency=0.5)

    topic = cast(str, convert(log['topics'][0]))
    if topic not in TOPICS:
        raise RuntimeError(f'sanity check? got invalid topic: {topic}')

    # print("Topic is", topic)

    event = TOPIC_TO_EVENT[topic]
    direction = TOPICS[topic]

    args = contract.events[event]().processLog(log)['args']

    if direction == Direction.OUT:
        kappa = w3.keccak(text=tx_hash.hex())

        def get_sent_info(_log: LogReceipt) -> Optional[Tuple[HexBytes, int]]:
            if _log['address'].lower() not in TOKENS_INFO[chain]:
                return None

            sent_token_address = HexBytes(_log['address'])
            sent_token = TOKENS_INFO[chain][sent_token_address.hex()]

            # TODO: test WETH transfers on other chains.
            if sent_token['symbol'] != 'WETH' and chain == 'ethereum':
                ret = sent_token['_contract'].events.Transfer()
                ret = ret.processLog(_log)
                sent_value = ret['args']['value']
            else:
                # Deposit (index_topic_1 address dst, uint256 wad)
                sent_value = int(_log['data'], 16)

            return sent_token_address, sent_value

        sent_token_address = sent_value = None

        for _log in receipt['logs']:
            ret = get_sent_info(_log)

            if ret is not None:
                sent_token_address, sent_value = ret
                break

        if sent_token_address is None or sent_value is None:
            raise RuntimeError(
                f'did not find sent_token_address or sent_value got: ',
                sent_token_address,
                sent_value,
            )

        if event in ['TokenDepositAndSwap', 'TokenRedeemAndSwap']:
            data = Events.TokenDepositAndSwap(args)
        elif event == 'TokenDeposit':
            data = Events.TokenDeposit(args)
        elif event == 'TokenRedeemAndRemove':
            data = Events.TokenRedeemAndRemove(args)
        elif event == 'TokenRedeem':
            data = Events.TokenRedeem(args)
        else:
            raise RuntimeError(
                f'did not converge OUT event: {event} {tx_hash.hex()} {chain}'
                f' args: {args}')

        txn = Transaction(tx_hash, None, HexBytes(tx_info['from']),
                          data.to, sent_value, None, True, from_chain,
                          data.chain_id, timestamp, None, None,
                          sent_token_address, None, kappa)

        # Store in DB
        if not testing:
            try:
                db: Database = MongoManager.get_db_instance()

                txn_with_kappa = db.transactions.find_one({'kappa': kappa.hex()})
                print(txn_with_kappa)

                if not txn_with_kappa:
                    # OUT received first. Store transaction as pending normally
                    db.transactions.insert_one(txn.serialize())
                    print(f"Inserted OUT transaction having with {kappa.hex()} txn hash {tx_hash.hex()}")

                else:
                    # IN already was received before OUT. Set missing values and unset pending
                    txn.pending = False
                    db.transactions.update_one(
                        filter={'kappa': kappa.hex()},
                        update={
                            "$set": {
                                **txn.serialize(),
                            }
                        }
                    )
                    print(f"Transaction matching complete. Updated OUT for transaction with kappa {kappa.hex()} txn hash {tx_hash.hex()}")

            except Exception as e:
                print("Error storing in DB!", e)

        return txn

    elif direction == Direction.IN:
        received_value = None
        kappa = args['kappa']

        if event in ['TokenWithdrawAndRemove', 'TokenMintAndSwap']:
            assert 'input' in tx_info  # IT EXISTS MYPY!
            _, inp_args = contract.decode_function_input(tx_info['input'])
            pool = get_pool_data(chain, inp_args['pool'])

            if event == 'TokenWithdrawAndRemove':
                data = Events.TokenWithdrawAndRemove(args)
            elif event == 'TokenMintAndSwap':
                data = Events.TokenMintAndSwap(args)
            else:
                # Will NEVER reach here - comprendo mypy???
                raise

            if data.swap_success:
                received_token = pool[data.token_idx_to]
            elif chain == 'ethereum':
                # nUSD (eth) - nexus assets are not in eth pools.
                received_token = '0x1b84765de8b7566e4ceaf4d0fd3c5af52d3dde4f'
            else:
                received_token = pool[0]

            received_token = HexBytes(received_token)
            swap_success = data.swap_success
        elif event in ['TokenWithdraw', 'TokenMint']:
            data = Events.TokenWithdraw(args)

            received_token = HexBytes(data.token)
            swap_success = None

            if event == 'TokenWithdraw':
                received_value = data.amount - data.fee
        else:
            raise RuntimeError(
                f'did not converge event IN: {event} {tx_hash.hex()} {chain} '
                f'args: {args}')

        if (chain in MISREPRESENTED_MAP
                and received_token in MISREPRESENTED_MAP[chain]):
            received_token = MISREPRESENTED_MAP[chain][received_token]

        if received_value is None:
            received_value = search_logs(chain, receipt,
                                         received_token)['value']

        if event == 'TokenMint':
            # emit TokenMint(to, token, amount.sub(fee), fee, kappa);
            if received_value != data.amount:  # type: ignore
                received_token, received_value = iterate_receipt_logs(
                    receipt,
                    check_factory(data.amount)  # type: ignore
                )

        # Must equal to False rather than eval to False since None is falsy.
        if swap_success == False:
            # The `received_value` we get earlier would be the initial bridged
            # amount without the fee excluded.
            received_value -= data.fee

        lost_txn = LostTransaction(tx_hash, data.to, received_value,
                                   from_chain, timestamp, received_token,
                                   swap_success, kappa)

        # Store in DB
        if not testing:
            try:
                db: Database = MongoManager.get_db_instance()
                txn_with_kappa = db.transactions.find_one({'kappa': kappa.hex()})

                # OUT already exists. Just set IN values and unset pending
                if txn_with_kappa:
                    db.transactions.update_one(
                        filter={'kappa': kappa.hex()},
                        update={
                            "$set": {
                                **lost_txn.serialize(),
                                "pending": False
                            }
                        }
                    )
                    print(f"Transaction matching complete. Updated IN for transaction with with kappa {kappa.hex()} txn hash {tx_hash.hex()}")
                else:
                    # IN txn shows up first
                    to_insert = lost_txn.serialize()
                    db.transactions.insert_one(to_insert)
                    print(f"Updated IN for transaction with kappa {kappa.hex()} txn hash {tx_hash.hex()}")

            except Exception as e:
                print("Error storing in DB!", e)

    if save_block_index:
        LOGS_REDIS_URL.set(f'{chain}:logs:{address}:MAX_BLOCK_STORED',
                           log['blockNumber'])
        LOGS_REDIS_URL.set(f'{chain}:logs:{address}:TX_INDEX',
                           log['transactionIndex'])


def get_logs(
        chain: str,
        callback: Callable[[str, str, LogReceipt], None],
        address: str,
        start_block: int = None,
        till_block: int = None,
        max_blocks: int = MAX_BLOCKS,
        topics: List[str] = list(TOPICS),
        key_namespace: str = 'logs',
        start_blocks: Dict[str, int] = _start_blocks,
) -> None:
    w3: Web3 = SYN_DATA[chain]['w3']
    _chain = f'[{chain}]'
    chain_len = max(len(c) for c in SYN_DATA) + 2
    tx_index = -1

    if start_block is None:
        _key_block = f'{chain}:{key_namespace}:{address}:MAX_BLOCK_STORED'
        _key_index = f'{chain}:{key_namespace}:{address}:TX_INDEX'

        if (ret := LOGS_REDIS_URL.get(_key_block)) is not None:
            start_block = max(int(ret), start_blocks[chain])

            if (ret := LOGS_REDIS_URL.get(_key_index)) is not None:
                tx_index = int(ret)
        else:
            start_block = start_blocks[chain]

    if till_block is None:
        till_block = w3.eth.block_number

    print(
        f'{key_namespace} | {_chain:{chain_len}} starting from {start_block} '
        f'with block height of {till_block}')

    jobs: List[gevent.Greenlet] = []
    _start = time.time()
    x = 0

    total_events = 0
    initial_block = start_block

    while start_block < till_block:
        to_block = min(start_block + max_blocks, till_block)

        params: FilterParams = {
            'fromBlock': start_block,
            'toBlock': to_block,
            'address': w3.toChecksumAddress(address),
            'topics': [topics],  # type: ignore
        }

        logs: List[LogReceipt] = w3.eth.get_logs(params)
        # Apparently, some RPC nodes don't bother
        # sorting events in a chronological order.
        # Let's sort them by block (from oldest to newest)
        # And by transaction index (within the same block,
        # also in ascending order)
        logs = sorted(
            logs,
            key=lambda k: (k['blockNumber'], k['transactionIndex']),
        )

        for log in logs:
            # Skip transactions from the very first block
            # that are already in the DB
            if log['blockNumber'] == initial_block \
                    and log['transactionIndex'] <= tx_index:
                continue

            retry(callback, chain, address, log)

        start_block += max_blocks + 1

        y = time.time() - _start
        total_events += len(logs)

        percent = 100 * (to_block - initial_block) \
                  / (till_block - initial_block)

        print(f'{key_namespace} | {_chain:{chain_len}} elapsed {y:5.1f}s'
              f' ({y - x:5.1f}s), found {total_events:5} events,'
              f' {percent:4.1f}% done: so far at block {start_block}')
        x = y

    gevent.joinall(jobs)
    print(f'{_chain:{chain_len}} it took {time.time() - _start:.1f}s!')
