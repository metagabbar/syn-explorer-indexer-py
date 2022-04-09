from typing import Dict, List, Literal, TypedDict, DefaultDict, cast
from collections import defaultdict
from enum import Enum
import json
import sys
import os

from web3.middleware.filter import local_filter_middleware
from web3.middleware.geth_poa import geth_poa_middleware
from dotenv import load_dotenv, find_dotenv
from gevent.greenlet import Greenlet
from web3.contract import Contract
from hexbytes import HexBytes
from gevent.pool import Pool
from web3 import Web3
import gevent
import redis

from indexer.contract import get_all_tokens_in_pool

# If `.env` exists, let it override the sample env file.
load_dotenv(find_dotenv('.env.sample'))
load_dotenv(override=True)

TESTING = "pytest" in sys.modules or os.getenv('TESTING')
if TESTING: print('Running with TESTING mode enabled.')

"""
Setup Redis
"""
REDIS_HOST = os.environ['REDIS_HOST']
REDIS_PORT = int(os.environ['REDIS_PORT'])
# We use this for processes to interact w/ eachother.
MESSAGE_QUEUE_REDIS_URL = f'redis://{REDIS_HOST}:{REDIS_PORT}/1'
MESSAGE_QUEUE_REDIS = redis.Redis.from_url(MESSAGE_QUEUE_REDIS_URL, decode_responses=True)
# We use this for storing eth_GetLogs and stuff related to that.
LOGS_REDIS_URL = redis.Redis(REDIS_HOST, REDIS_PORT, decode_responses=True)
"""
Load ABIs
"""
ERC20_BARE_ABI = """[{"constant":true,"inputs":[],"name":"name","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint8"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"symbol","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"from","type":"address"},{"indexed":true,"internalType":"address","name":"to","type":"address"},{"indexed":false,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Transfer","type":"event"}]"""
BASEPOOL_ABI = """[{"inputs":[{"internalType":"uint8","name":"index","type":"uint8"}],"name":"getToken","outputs":[{"internalType":"contract IERC20","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"index","type":"uint256"}],"name":"getAdminBalance","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"getVirtualPrice","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"}]"""

_abis_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'abis')
with open(os.path.join(_abis_path, 'bridge.json')) as f:
    BRIDGE_ABI = json.load(f)['abi']
with open(os.path.join(_abis_path, 'pool.json')) as f:
    POOL_ABI = json.load(f)['abi']
with open(os.path.join(_abis_path, 'bridgeConfig.json')) as f:
    BRIDGE_CONFIG_ABI = json.load(f)['abi']

"""
Chain ids to names
"""
CHAINS = {
    1: 'ethereum',
    56: 'bsc',
}
CHAINS_REVERSED = {v: k for k, v in CHAINS.items()}

"""
Chain names mapped to synapse bridges and pools
"""
SYN_DATA = {
    "ethereum": {
        "rpc": os.getenv('ETH_RPC'),
        "bridge": "0x2796317b0ff8538f253012862c06787adfb8ceb6",
        "nusdpool": "0x1116898DdA4015eD8dDefb84b6e8Bc24528Af2d8",
    },
    "bsc": {
        "rpc": os.getenv('BSC_RPC'),
        "bridge": "0xd123f70ae324d34a9e76b67a27bf77593ba8749f",
        "nusdpool": "0x28ec0b36f0819ecb5005cab836f4ed5a2eca4d13",
    }
}

# Init 'func' to append `contract` to SYN_DATA so we can call the ABI simpler later.
for key, value in SYN_DATA.items():
    w3 = Web3(Web3.HTTPProvider(value['rpc']))
    assert w3.isConnected(), key

    if key != 'ethereum':
        w3.middleware_onion.inject(geth_poa_middleware, layer=0)

    w3.middleware_onion.add(local_filter_middleware)
    print(key)
    try:
        print(w3.eth.syncing)
    except Exception as e:
        print(e)

    value.update({'w3': w3})

    if value.get('nusdpool') is not None:
        value.update({
            'nusdpool_contract':
                w3.eth.contract(Web3.toChecksumAddress(value['nusdpool']),
                                abi=BASEPOOL_ABI)
        })

    if value.get('nethpool') is not None:
        value.update({
            'nethpool_contract':
                w3.eth.contract(Web3.toChecksumAddress(value['nethpool']),
                                abi=BASEPOOL_ABI)
        })


"""
In a bridging scenario, there are txns out of a chain and into a chain
We track direction as sometimes, due to RPC lag etc, OUT transactions
appear before IN transactions.
"""
class Direction(Enum):
    def __str__(self) -> str:
        return self.name

    OUT = 0
    IN = 1


EVENTS = {
    'TokenRedeemAndSwap': Direction.OUT,
    'TokenMintAndSwap': Direction.IN,
    'TokenRedeemAndRemove': Direction.OUT,
    'TokenRedeem': Direction.OUT,
    'TokenMint': Direction.IN,
    'TokenDepositAndSwap': Direction.OUT,
    'TokenWithdrawAndRemove': Direction.IN,
    'TokenDeposit': Direction.OUT,
    'TokenWithdraw': Direction.IN,
}

TOPICS = {
    # event TokenRedeemAndSwap(
    #  address indexed to,
    #  uint256 chainId,
    #  IERC20 token,
    #  uint256 amount,
    #  uint8 tokenIndexFrom,
    #  uint8 tokenIndexTo,
    #  uint256 minDy,
    #  uint256 deadline
    # );
    '0x91f25e9be0134ec851830e0e76dc71e06f9dade75a9b84e9524071dbbc319425':
        Direction.OUT,
    # event TokenMintAndSwap(
    #  address indexed to,
    #  IERC20Mintable token,
    #  uint256 amount,
    #  uint256 fee,
    #  uint8 tokenIndexFrom,
    #  uint8 tokenIndexTo,
    #  uint256 minDy,
    #  uint256 deadline,
    #  bool swapSuccess,
    #  bytes32 indexed kappa
    # );
    '0x4f56ec39e98539920503fd54ee56ae0cbebe9eb15aa778f18de67701eeae7c65':
        Direction.IN,
    # event TokenRedeemAndRemove(
    #  address indexed to,
    #  uint256 chainId,
    #  IERC20 token,
    #  uint256 amount,
    #  uint8 swapTokenIndex,
    #  uint256 swapMinAmount,
    #  uint256 swapDeadline
    # );
    '0x9a7024cde1920aa50cdde09ca396229e8c4d530d5cfdc6233590def70a94408c':
        Direction.OUT,
    # event TokenRedeem(
    #  address indexed to,
    #  uint256 chainId,
    #  IERC20 token,
    #  uint256 amount
    # );
    '0xdc5bad4651c5fbe9977a696aadc65996c468cde1448dd468ec0d83bf61c4b57c':
        Direction.OUT,
    # event TokenMint(
    #  address indexed to,
    #  IERC20Mintable token,
    #  uint256 amount,
    #  uint256 fee,
    #  bytes32 indexed kappa
    # );
    '0xbf14b9fde87f6e1c29a7e0787ad1d0d64b4648d8ae63da21524d9fd0f283dd38':
        Direction.IN,
    # event TokenDepositAndSwap(
    #  address indexed to,
    #  uint256 chainId,
    #  IERC20 token,
    #  uint256 amount,
    #  uint8 tokenIndexFrom,
    #  uint8 tokenIndexTo,
    #  uint256 minDy,
    #  uint256 deadline
    # );
    '0x79c15604b92ef54d3f61f0c40caab8857927ca3d5092367163b4562c1699eb5f':
        Direction.OUT,
    # event TokenWithdrawAndRemove(
    #  address indexed to,
    #  IERC20 token,
    #  uint256 amount,
    #  uint256 fee,
    #  uint8 swapTokenIndex,
    #  uint256 swapMinAmount,
    #  uint256 swapDeadline,
    #  bool swapSuccess,
    #  bytes32 indexed kappa
    # );
    '0xc1a608d0f8122d014d03cc915a91d98cef4ebaf31ea3552320430cba05211b6d':
        Direction.IN,
    # event TokenDeposit(
    #  address indexed to,
    #  uint256 chainId,
    #  IERC20 token,
    #  uint256 amount
    # );
    '0xda5273705dbef4bf1b902a131c2eac086b7e1476a8ab0cb4da08af1fe1bd8e3b':
        Direction.OUT,
    # event TokenWithdraw(
    #  address indexed to,
    #  IERC20 token,
    #  uint256 amount,
    #  uint256 fee,
    #  bytes32 indexed kappa
    # );
    '0x8b0afdc777af6946e53045a4a75212769075d30455a212ac51c9b16f9c5c9b26':
        Direction.IN,
}

TOPIC_TO_EVENT = dict(zip(TOPICS.keys(), EVENTS.keys()))

MAX_UINT8 = 2 ** 8 - 1
SYN_DECIMALS = 18

TOKENS = {
    'ethereum': [
        '0x71ab77b7dbb4fa7e017bc15090b2163221420282',  # HIGH
        '0x0f2d719407fdbeff09d87557abb7232601fd9f29',  # SYN
        '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',  # WETH
        '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',  # USDC
        '0x6b175474e89094c44da98b954eedeac495271d0f',  # DAI
        '0xdac17f958d2ee523a2206206994597c13d831ec7',  # USDT
        '0x1b84765de8b7566e4ceaf4d0fd3c5af52d3dde4f',  # nUSD
        '0xbaac2b4491727d78d2b78815144570b9f2fe8899',  # DOG
        '0x853d955acef822db058eb8505911ed77f175b99e',  # FRAX
        '0xca76543cf381ebbb277be79574059e32108e3e65',  # wsOHM
        '0x0ab87046fbb341d058f17cbc4c1133f25a20a52f',  # gOHM
        '0x0261018Aa50E28133C1aE7a29ebdf9Bd21b878Cb',  # UST
        '0x98585dFc8d9e7D48F0b1aE47ce33332CF4237D96',  # NEWO
        '0x73968b9a57c6e53d41345fd57a6e6ae27d6cdb2f',  # SDT
        '0x02b5453d92b730f29a86a0d5ef6e930c4cf8860b',  # USDB
    ],
    'bsc': [
        '0x23b891e5c62e0955ae2bd185990103928ab817b3',  # nUSD
        '0xf0b8b631145d393a767b4387d08aa09969b2dfed',  # USD-LP
        '0xe9e7cea3dedca5984780bafc599bd69add087d56',  # BUSD
        '0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d',  # USDC
        '0xaa88c603d142c371ea0eac8756123c5805edee03',  # DOG
        '0x55d398326f99059ff775485246999027b3197955',  # USDT
        '0x5f4bde007dc06b867f86ebfe4802e34a1ffeed63',  # HIGH
        '0xa4080f1778e69467e905b8d6f72f6e441f9e9484',  # SYN
        '0x42f6f551ae042cbe50c739158b4f0cac0edb9096',  # NRV
        '0x130025ee738a66e691e6a7a62381cb33c6d9ae83',  # JUMP
        '0x0fe9778c005a5a6115cbe12b0568a2d50b765a51',  # NFD
        '0xc13b7a43223bb9bf4b69bd68ab20ca1b79d81c75',  # JGN
        '0x88918495892baf4536611e38e75d771dc6ec0863',  # gOHM
        '0xb7A6c5f0cc98d24Cf4B2011842e64316Ff6d042c',  # UST
        '0xc8699abbba90c7479dedccef19ef78969a2fc608',  # USDB
    ]
}

MISREPRESENTED_MAP: Dict[str, Dict[HexBytes, HexBytes]] = defaultdict(dict)

# GMX WRAPPER -> GMX, GMX is not ERC20 compatible
MISREPRESENTED_MAP['avalanche'] \
    [HexBytes('0x20A9DC684B4d0407EF8C9A302BEAaA18ee15F656')] \
    = HexBytes('0x62edc0692BD897D2295872a9FFCac5425011c661')


class TokenInfo(TypedDict):
    _contract: Contract
    name: str
    decimals: int
    symbol: str


TOKENS_INFO: Dict[str, Dict[str, TokenInfo]] = defaultdict(dict)
__jobs: List[Greenlet] = []


def __cb(w3: Web3, chain: str, token: str) -> None:
    contract = w3.eth.contract(w3.toChecksumAddress(token), abi=ERC20_BARE_ABI)

    decimals = contract.functions.decimals().call()
    name = contract.functions.name().call()
    symbol = contract.functions.symbol().call()

    TOKENS_INFO[chain].update({
        token.lower():
            TokenInfo(_contract=contract,
                      name=name,
                      symbol=symbol,
                      decimals=decimals)
    })


__pool = Pool(size=24)
for chain, tokens in TOKENS.items():
    w3: Web3 = SYN_DATA[chain]['w3']

    for token in tokens:
        assert token not in TOKENS_INFO[chain], \
            f'duped token? {token} @ {chain} | {TOKENS_INFO[chain][token]}'

        __jobs.append(__pool.spawn(__cb, w3, chain, token))

gevent.joinall(__jobs, raise_error=True)

TOKEN_DECIMALS: Dict[str, Dict[str, int]] = defaultdict(dict)
TOKEN_SYMBOLS: Dict[str, Dict[str, str]] = defaultdict(dict)

# `TOKEN_DECIMALS` is an abstraction of `TOKENS_INFO`.
for chain, v in TOKENS_INFO.items():
    for token, data in v.items():
        assert token not in TOKEN_DECIMALS[chain], \
            f'duped token? {token} @ {chain} | {TOKEN_DECIMALS[chain][token]}'

        TOKEN_SYMBOLS[chain].update({token: data['symbol']})
        TOKEN_DECIMALS[chain].update({token: data['decimals']})

_TKS = DefaultDict[str, Dict[Literal['nusd', 'neth'], Dict[int, str]]]
#: Example schema:
#: {'arbitrum':
#:   {'neth': {0: '0x3ea9B0ab55F34Fb188824Ee288CeaEfC63cf908e',
#:             1: '0x82aF49447D8a07e3bd95BD0d56f35241523fBab1'},
#:    'nusd': {0: '0x2913E812Cf0dcCA30FB28E6Cac3d2DCFF4497688',
#:             1: '0xFEa7a6a0B346362BF88A9e4A88416B77a57D6c2A',
#:             2: '0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8',
#:             3: '0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9'}}
TOKENS_IN_POOL: _TKS = defaultdict(lambda: defaultdict(dict))

for chain, v in SYN_DATA.items():
    if 'nusdpool_contract' in v:
        ret = get_all_tokens_in_pool(chain)

        for i, token in enumerate(ret):
            TOKENS_IN_POOL[chain]['nusd'].update({i: token})

    if 'nethpool_contract' in v:
        ret = get_all_tokens_in_pool(chain, func='nethpool_contract')

        for i, token in enumerate(ret):
            TOKENS_IN_POOL[chain]['neth'].update({i: token})

POOLS: Dict[str, Dict[Literal['nusd', 'neth'], str]] = {
    'ethereum': {
        'nusd': '0x1116898dda4015ed8ddefb84b6e8bc24528af2d8',
    },
    'bsc': {
        'nusd': '0x28ec0b36f0819ecb5005cab836f4ed5a2eca4d13',
    }
}

# V2
BRIDGE_CONFIG = cast(Web3, SYN_DATA['ethereum']['w3']).eth.contract(
    Web3.toChecksumAddress('0xAE908bb4905bcA9BdE0656CC869d0F23e77875E7'),
    abi=BRIDGE_CONFIG_ABI,
)
