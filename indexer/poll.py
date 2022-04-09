from typing import List, Callable, TypeVar, cast

from web3.types import LogReceipt
from gevent import Greenlet
from web3 import Web3
import gevent

from indexer.data import TOPICS, SYN_DATA
from indexer.helpers import retry

# NOTE: :type:`EventData` is not really :type:`LogReceipt`,
# but close enough to assume its type.
CB = Callable[[str, str, LogReceipt], None]
T = TypeVar('T')


def log_loop(filter, chain: str, address: str, poll: int, cb: CB):
    while True:
        try:
            # `event` is of type `EventData`.
            for event in filter.get_new_entries():
                print("New event")
                retry(cb, chain, address, event, save_block_index=False)
        except Exception as e:
            print(f'err filter log_loop: {e}')
        finally:
            gevent.sleep(poll)


def start(cb: CB) -> None:
    jobs: List[Greenlet] = []

    for chain, x in SYN_DATA.items():
        _address = Web3.toChecksumAddress(x['bridge'])

        filter = cast(Web3, x['w3']).eth.filter({
            'address': _address,
            'fromBlock': 'latest',
            'topics': [list(TOPICS)],
        })

        jobs.append(
            gevent.spawn(
                log_loop,
                filter,
                chain,
                _address,
                poll=2,
                cb=cb,
            ))

    # This will never sanely finish.
    gevent.joinall(jobs)
