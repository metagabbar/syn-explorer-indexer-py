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
# We use this for storing eth_GetLogs and stuff related to that.
LOGS_REDIS_URL = redis.from_url(os.environ['REDIS_URL'], decode_responses=True)
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
    43114: 'avalanche',
    1666600000: 'harmony',
    42161: 'arbitrum',
    250: 'fantom',
    137: 'polygon',
    56: 'bsc',
    1: 'ethereum',
    288: 'boba',
    1285: 'moonriver',
    10: 'optimism',
    1313161554: 'aurora',
    1284: 'moonbeam',
    25: 'cronos',
    1088: 'metis',
    53935: 'dfk',
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
        "avalanche": {
        "rpc": os.getenv('AVAX_RPC'),
        "bridge": "0xc05e61d0e7a63d27546389b7ad62fdff5a91aace",
        "nusdpool": "0xed2a7edd7413021d440b09d654f3b87712abab66",
        "nethpool": "0x77a7e60555bC18B4Be44C181b2575eee46212d44",
    },
    "bsc": {
        "rpc": os.getenv('BSC_RPC'),
        "bridge": "0xd123f70ae324d34a9e76b67a27bf77593ba8749f",
        "nusdpool": "0x28ec0b36f0819ecb5005cab836f4ed5a2eca4d13",
    },
    "polygon": {
        "rpc": os.getenv('POLYGON_RPC'),
        "bridge": "0x8f5bbb2bb8c2ee94639e55d5f41de9b4839c1280",
        "nusdpool": "0x85fcd7dd0a1e1a9fcd5fd886ed522de8221c3ee5",
    },
    "arbitrum": {
        "rpc": os.getenv('ARB_RPC'),
        "bridge": "0x6f4e8eba4d337f874ab57478acc2cb5bacdc19c9",
        "nusdpool": "0x0db3fe3b770c95a0b99d1ed6f2627933466c0dd8",
        "nethpool": "0xa067668661c84476afcdc6fa5d758c4c01c34352",
    },
    "fantom": {
        "rpc": os.getenv('FTM_RPC'),
        "bridge": "0xaf41a65f786339e7911f4acdad6bd49426f2dc6b",
        "nusdpool": "0x2913e812cf0dcca30fb28e6cac3d2dcff4497688",
        "nethpool": "0x8d9ba570d6cb60c7e3e0f31343efe75ab8e65fb1",
    },
    "harmony": {
        "rpc": os.getenv('HARMONY_RPC'),
        "bridge": "0xaf41a65f786339e7911f4acdad6bd49426f2dc6b",
        "nusdpool": "0x3ea9b0ab55f34fb188824ee288ceaefc63cf908e",
        "nethpool": "0x2913e812cf0dcca30fb28e6cac3d2dcff4497688",
    },
    "boba": {
        "rpc": os.getenv('BOBA_RPC'),
        "bridge": "0x432036208d2717394d2614d6697c46df3ed69540",
        "nusdpool": "0x75ff037256b36f15919369ac58695550be72fead",
        "nethpool": "0x753bb855c8fe814233d26bb23af61cb3d2022be5",
    },
    "moonriver": {
        "rpc": os.getenv('MOVR_RPC'),
        "bridge": "0xaed5b25be1c3163c907a471082640450f928ddfe",
    },
    "optimism": {
        "rpc": os.getenv('OPTIMISM_RPC'),
        "bridge": "0xaf41a65f786339e7911f4acdad6bd49426f2dc6b",
        "nethpool": "0xe27bff97ce92c3e1ff7aa9f86781fdd6d48f5ee9",
    },
    "aurora": {
        "rpc": os.getenv('AURORA_RPC'),
        "bridge": "0xaed5b25be1c3163c907a471082640450f928ddfe",
        "nusdpool": "0xcef6c2e20898c2604886b888552ca6ccf66933b0",
    },
    "moonbeam": {
        "rpc": os.getenv('MOONBEAM_RPC'),
        'bridge': '0x84a420459cd31c3c34583f67e0f0fb191067d32f',
    },
    "cronos": {
        "rpc": os.getenv('CRONOS_RPC'),
        "bridge": "0xe27bff97ce92c3e1ff7aa9f86781fdd6d48f5ee9",
    },
    "metis": {
        "rpc": os.getenv('METIS_RPC'),
        "bridge": "0x06fea8513ff03a0d3f61324da709d4cf06f42a5c",
    },
    "dfk": {
        "rpc": os.getenv('DFK_RPC'),
        "bridge": "0xe05c976d3f045d0e6e7a6f61083d98a15603cf6a",
    },
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
    ],
    'polygon': [
        '0xf8f9efc0db77d8881500bb06ff5d6abc3070e695',  # SYN
        '0x8f3cf7ad23cd3cadbd9735aff958023239c6a063',  # DAI
        '0x2791bca1f2de4661ed88a30c99a7a9449aa84174',  # USDC
        '0xc2132d05d31c914a87c6611c10748aeb04b58e8f',  # USDT
        '0xb6c473756050de474286bed418b77aeac39b02af',  # nUSD
        '0x128a587555d1148766ef4327172129b50ec66e5d',  # USD-LP
        '0x0a5926027d407222f8fe20f24cb16e103f617046',  # NFD
        '0xd8ca34fd379d9ca3c6ee3b3905678320f5b45195',  # gOHM
        '0xeee3371b89fc43ea970e908536fcddd975135d8a',  # DOG
        '0x48a34796653afdaa1647986b33544c911578e767',  # synFRAX
        '0x7ceb23fd6bc0add59e62ac25578270cff1b9f619',  # WETH
        '0x565098CBa693b3325f9fe01D41b7A1cd792Abab1',  # UST
        '0xfa1fbb8ef55a4855e5688c0ee13ac3f202486286',  # USDB
    ],
    'avalanche': [
        '0xd586e7f844cea2f87f50152665bcbc2c279d8d70',  # DAI
        '0xa7d7079b0fead91f3e65f86e8915cb59c1a4c664',  # USDC
        '0xc7198437980c041c805a1edcba50c1ce5db95118',  # USDT
        '0xcfc37a6ab183dd4aed08c204d1c2773c0b1bdf46',  # nUSD
        '0x55904f416586b5140a0f666cf5acf320adf64846',  # USD-LP
        '0x1f1e7c893855525b303f99bdf5c3c05be09ca251',  # SYN
        '0xf1293574ee43950e7a8c9f1005ff097a9a713959',  # NFD
        '0x19e1ae0ee35c0404f835521146206595d37981ae',  # nETH
        '0x321e7092a180bb43555132ec53aaa65a5bf84251',  # gOHM
        '0xcc5672600b948df4b665d9979357bef3af56b300',  # synFRAX
        '0x53f7c5869a859f0aec3d334ee8b4cf01e3492f21',  # avWETH
        '0x49D5c2BdFfac6CE2BFdB6640F4F80f226bc10bAB',  # WETH.e
        '0x62edc0692bd897d2295872a9ffcac5425011c661',  # GMX
        '0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7',  # WAVAX
        '0xE97097dE8d6A17Be3c39d53AE63347706dCf8f43',  # UST
        '0x4Bfc90322dD638F81F034517359BD447f8E0235a',  # NEWO
        '0xccbf7c451f81752f7d2237f2c18c371e6e089e69',  # SDT
        '0x5ab7084cb9d270c2cb052dd30dbecbca42f8620c',  # USDB
        '0x997ddaa07d716995de90577c123db411584e5e46',  # JEWEL
    ],
    'arbitrum': [
        '0xda10009cbd5d07dd0cecc66161fc93d7c9000da1',  # DAI
        '0x080f6aed32fc474dd5717105dba5ea57268f46eb',  # SYN
        '0xff970a61a04b1ca14834a43f5de4533ebddb5cc8',  # USDC
        '0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9',  # USDT
        '0x2913e812cf0dcca30fb28e6cac3d2dcff4497688',  # nUSD
        '0xe264cb5a941f98a391b9d5244378edf79bf5c19e',  # USD-LP
        '0xfea7a6a0b346362bf88a9e4a88416b77a57d6c2a',  # MIM
        '0x3ea9b0ab55f34fb188824ee288ceaefc63cf908e',  # nETH
        '0x82af49447d8a07e3bd95bd0d56f35241523fbab1',  # WETH
        '0x8d9ba570d6cb60c7e3e0f31343efe75ab8e65fb1',  # gOHM
        '0x85662fd123280827e11c59973ac9fcbe838dc3b4',  # synFRAX
        '0xfc5a1a6eb076a2c7ad06ed22c90d7e710e35ad0a',  # GMX
        '0x13780E6d5696DD91454F6d3BbC2616687fEa43d0',  # UST
        '0x0877154a755B24D499B8e2bD7ecD54d3c92BA433',  # NEWO
        '0x1a4da80967373fd929961e976b4b53ceec063a15',  # LUNA
    ],
    'fantom': [
        '0x04068da6c83afcfa0e13ba15a6696662335d5b75',  # USDC
        '0x049d68029688eabf473097a2fc38ef61633a3c7a',  # fUSDT
        '0x43cf58380e69594fa2a5682de484ae00edd83e94',  # USD-LP
        '0x82f0b8b456c1a451378467398982d4834b6829c1',  # MIM
        '0xed2a7edd7413021d440b09d654f3b87712abab66',  # nUSD
        '0xe55e19fb4f2d85af758950957714292dac1e25b2',  # SYN
        '0x78de9326792ce1d6eca0c978753c6953cdeedd73',  # JUMP
        '0x91fa20244fb509e8289ca630e5db3e9166233fdc',  # gOHM
        '0x1852f70512298d56e9c8fdd905e02581e04ddb2a',  # synFRAX
        '0x67c10c397dd0ba417329543c1a40eb48aaa7cd00',  # nETH
        '0x74b23882a30290451a17c44f4f05243b6b58c76d',  # WETH
        '0xa0554607e477cdC9d0EE2A6b087F4b2DC2815C22',  # UST
        '0xe3c82a836ec85311a433fbd9486efaf4b1afbf48',  # SDT
        '0x6fc9383486c163fa48becdec79d6058f984f62ca',  # USDB
    ],
    'harmony': [
        '0xe55e19fb4f2d85af758950957714292dac1e25b2',  # SYN
        '0xef977d2f931c1978db5f6747666fa1eacb0d0339',  # 1DAI
        '0x985458e523db3d53125813ed68c274899e9dfab4',  # 1USDC
        '0x3c2b8be99c50593081eaa2a724f0b8285f5aba8f',  # 1USDT
        '0xed2a7edd7413021d440b09d654f3b87712abab66',  # nUSD
        '0xcf664087a5bb0237a0bad6742852ec6c8d69a27a',  # ONE
        '0x1852f70512298d56e9c8fdd905e02581e04ddb2a',  # synFRAX
        '0xfa7191d292d5633f702b0bd7e3e3bccc0e633200',  # old synFRAX
        '0x67c10c397dd0ba417329543c1a40eb48aaa7cd00',  # gOHM
        '0x0b5740c6b4a97f90ef2f0220651cca420b868ffb',  # nETH
        '0x6983d1e6def3690c4d616b13597a09e6193ea013',  # 1ETH
        '0xa0554607e477cdC9d0EE2A6b087F4b2DC2815C22',  # UST
        '0xe3c82a836ec85311a433fbd9486efaf4b1afbf48',  # SDT
        '0xd9eaa386ccd65f30b77ff175f6b52115fe454fd6',  # AVAX
        '0x28b42698caf46b4b012cf38b6c75867e0762186d',  # synJEWEL
        '0xa9ce83507d872c5e1273e745abcfda849daa654f',  # xJEWEL
        '0x72cb10c6bfa5624dd07ef608027e366bd690048f',  # JEWEL
    ],
    'boba': [
        '0x66a2a913e447d6b4bf33efbec43aaef87890fbbc',  # USDC
        '0xb554a55358ff0382fb21f0a478c3546d1106be8c',  # SYN
        '0x5de1677344d3cb0d7d465c10b72a8f60699c062d',  # USDT
        '0xdeaddeaddeaddeaddeaddeaddeaddeaddead0000',  # WETH
        '0x96419929d7949d6a801a6909c145c8eef6a40431',  # nETH
        '0x6b4712ae9797c199edd44f897ca09bc57628a1cf',  # nUSD
        '0xf74195bb8a5cf652411867c5c2c5b8c2a402be35',  # DAI
        '0xd203de32170130082896b4111edf825a4774c18e',  # WETH
        '0xd22c0a4af486c7fa08e282e9eb5f30f9aaa62c95',  # gOHM
        '0x037527278b4ac8a4327e7015b788001c2954cf82',  # WETH
        '0x61A269a9506272D128d79ABfE8E8276570967f00',  # UST
    ],
    'moonriver': [
        '0xd80d8688b02b3fd3afb81cdb124f188bb5ad0445',  # SYN
        '0xe96ac70907fff3efee79f502c985a7a21bce407d',  # synFRAX
        '0x1a93b23281cc1cde4c4741353f3064709a16197d',  # FRAX
        '0x3bf21ce864e58731b6f28d68d5928bcbeb0ad172',  # gOHM
        '0x76906411d07815491a5e577022757ad941fb5066',  # veSOLAR
        '0x98878b06940ae243284ca214f92bb71a2b032b8a',  # WMOVR
        '0xa9D0C0E124F53f4bE1439EBc35A9C73c0e8275fB',  # UST
        '0x3e193c39626bafb41ebe8bdd11ec7cca9b3ec0b2',  # USDB
    ],
    'optimism': [
        '0x809dc529f07651bd43a172e8db6f4a7a0d771036',  # nETH
        '0x5a5fff6f753d7c11a56a52fe47a177a87e431655',  # SYN
        '0x121ab82b49b2bc4c7901ca46b8277962b4350204',  # WETH
        '0x4200000000000000000000000000000000000006',  # WETH
        '0xFB21B70922B9f6e3C6274BcD6CB1aa8A0fe20B80',  # UST
        '0x0b5740c6b4a97f90ef2f0220651cca420b868ffb',  # gOHM
        '0x931b8f17764362a3325d30681009f0edd6211231',  # LUNA
    ],
    'aurora': [
        '0xd80d8688b02b3fd3afb81cdb124f188bb5ad0445',  # SYN
        '0xb12bfca5a55806aaf64e99521918a4bf0fc40802',  # USDC
        '0x4988a896b1227218e4a686fde5eabdcabd91571f',  # USDT
        '0x07379565cd8b0cae7c60dc78e7f601b34af2a21c',  # nUSD
        '0xb1Da21B0531257a7E5aEfa0cd3CbF23AfC674cE1',  # UST
    ],
    'moonbeam': [
        '0x3192ae73315c3634ffa217f71cf6cbc30fee349a',  # WETH
        '0xbf180c122d85831dcb55dc673ab47c8ab9bcefb4',  # nETH
        '0xf44938b0125a6662f9536281ad2cd6c499f22004',  # SYN
        '0x0db6729c03c85b0708166ca92801bcb5cac781fc',  # veSOLAR
        '0xd2666441443daa61492ffe0f37717578714a4521',  # gOHM
        '0xdd47a348ab60c61ad6b60ca8c31ea5e00ebfab4f',  # synFRAX
        '0x1d4c2a246311bb9f827f4c768e277ff5787b7d7e',  # MOVR
        '0xa1f8890e39b4d8e33efe296d698fe42fb5e59cc3',  # AVAX
        '0x5CF84397944B9554A278870B510e86667681ff8D',  # UST
    ],
    'cronos': [
        '0x7Bb5c7e3bF0B2a28fA26359667110bB974fF9359',  # UST
        '0xfd0f80899983b8d46152aa1717d76cba71a31616',  # SYN
        '0x396c9c192dd323995346632581bef92a31ac623b',  # nUSD
        '0xbb0a63a6ca2071c6c4bcac11a1a317b20e3e999c',  # gOHM
    ],
    'metis': [
        '0x0b5740c6b4a97f90eF2F0220651Cca420B868FfB',  # UST
        '0xfb21b70922b9f6e3c6274bcd6cb1aa8a0fe20b80',  # gOHM
        '0x961318fc85475e125b99cc9215f62679ae5200ab',  # nUSD
        '0x67c10c397dd0ba417329543c1a40eb48aaa7cd00',  # SYN
        '0xea32a96608495e54156ae48931a7c20f0dcc1a21',  # USDC
        '0x931b8f17764362a3325d30681009f0edd6211231',  # nETH
        '0x420000000000000000000000000000000000000a',  # WETH
    ],
    'dfk': [
        '0xb57b60debdb0b8172bb6316a9164bd3c695f133a',  # AVAX
        '0xccb93dabd71c8dad03fc4ce5559dc3d89f67a260',  # WJEWEL
        '0x9596a3c6a4b2597adcc5d6d69b281a7c49e3fe6a',  # nETH
        '0x52285d426120ab91f378b3df4a15a036a62200ae',  # nUSD
        '0x77f2656d04e158f915bc22f07b779d94c1dc47ff',  # xJEWEL
        '0xb6b5c854a8f71939556d4f3a2e5829f7fcc1bf2a',  # SYN
	    '0x3ad9dfe640e1a9cc1d9b0948620820d975c3803a',  # USDC
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
    },
    'polygon': {
        'nusd': '0x85fcd7dd0a1e1a9fcd5fd886ed522de8221c3ee5',
    },
    'arbitrum': {
        'nusd': '0x0db3fe3b770c95a0b99d1ed6f2627933466c0dd8',
        'neth': '0xa067668661c84476afcdc6fa5d758c4c01c34352',
    },
    'fantom': {
        'nusd': '0x2913e812cf0dcca30fb28e6cac3d2dcff4497688',
    },
    'harmony': {
        'nusd': '0x3ea9b0ab55f34fb188824ee288ceaefc63cf908e',
    },
    'boba': {
        'nusd': '0x75ff037256b36f15919369ac58695550be72fead',
        'neth': '0x753bb855c8fe814233d26bb23af61cb3d2022be5',
    },
    'optimism': {
        'neth': '0xe27bff97ce92c3e1ff7aa9f86781fdd6d48f5ee9',
    },
}

# V2
BRIDGE_CONFIG = cast(Web3, SYN_DATA['ethereum']['w3']).eth.contract(
    Web3.toChecksumAddress('0xAE908bb4905bcA9BdE0656CC869d0F23e77875E7'),
    abi=BRIDGE_CONFIG_ABI,
)
