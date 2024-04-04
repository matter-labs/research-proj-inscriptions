import web3
import polars as pl
from tqdm import tqdm


class Utils:

    def __init__(self, zkSync_data_dir, data_dir='../data/'):
        # Existing dataset
        self.data_dir = data_dir
        self.data_path = self.create_data_path(zkSync_data_dir)

    def get_data_path(self):
        return self.data_path

    def create_data_path(self, path_dir):
        data_path = dict()
        data_path['blocks'] = path_dir+'blocks_*.parquet.gz'
        data_path['transactions'] = path_dir+'transactions_*.parquet.gz'
        data_path['tx_receipts'] = path_dir+'tx_receipts_*.parquet.gz'
        data_path['logs'] = path_dir+'logs_*.parquet.gz'
        return data_path

    @staticmethod
    def check_sig(sig, topic_0):
        return web3.Web3.keccak(text=sig).hex() == topic_0

    @staticmethod
    def parse_addresses(address):
        if isinstance(address, str):
            return '0x' + address[-40:].lower()
        return address

    @staticmethod
    def parse_amount(amount):
        if isinstance(amount, str):
            return int(amount, 16)
        return amount

    @staticmethod
    def decode_input_data(input_data):
        try:
            response = web3.Web3.to_text(input_data)
        except:
            response = None
        return response

    def get_txs(self, inscriptions_tag):
        txs = (pl.scan_parquet(self.data_path['transactions'])
               # .filter(pl.col('from').eq(pl.col('to')) & pl.col('input').str.starts_with(inscriptions_tag))
               .filter(pl.col('input').str.starts_with(inscriptions_tag))
               .select([
                   pl.col('blockNumber').alias('block_number'),
                   pl.col('hash').alias('tx_hash'),
                   pl.col('input').alias('tx_input_data'),
                   pl.col('from').str.to_lowercase().alias('issuer'),
                   pl.col('to').str.to_lowercase().alias('receiver'),
               ])
               )
        blocks = (
            pl.scan_parquet(self.data_path['blocks'])
            .select(pl.col('number').alias('block_number'), pl.from_epoch(pl.col('timestamp')))
        )
        q = txs.join(blocks, left_on='block_number',
                     right_on='block_number', how='left')

        return q.collect(streaming=True)

    def get_receipts(self, wallet_addresses):
        q = (pl.scan_parquet(self.data_path['tx_receipts'])
             # .filter(pl.col('from').eq(pl.col('to')))
             # .filter(pl.col('from').str.to_lowercase().is_in(wallet_addresses) | pl.col('to').str.to_lowercase().is_in(wallet_addresses))
             .filter(pl.col('from').str.to_lowercase().is_in(wallet_addresses))
             .select([
                 pl.col('transactionHash').alias('tx_hash'),
                 pl.col('gasUsed').alias('gas_used'),
                 pl.col('effectiveGasPrice').alias('gas_effective_price'),
                 pl.col('gasUsed').mul(
                     pl.col('effectiveGasPrice')).truediv(1e18).alias('fees'),
                 pl.col('status').alias('tx_status'),
             ])
             )
        return q.collect(streaming=True)

    def get_txs(self, inscriptions_tag):
        txs = (pl.scan_parquet(self.data_path['transactions'])
               # .filter(pl.col('from').eq(pl.col('to')) & pl.col('input').str.starts_with(inscriptions_tag))
               .filter(pl.col('input').str.starts_with(inscriptions_tag))
               .select([
                   pl.col('blockNumber').alias('block_number'),
                   pl.col('hash').alias('tx_hash'),
                   pl.col('input').alias('tx_input_data'),
                   pl.col('from').str.to_lowercase().alias('issuer'),
                   pl.col('to').str.to_lowercase().alias('receiver'),
               ])
               )
        blocks = (
            pl.scan_parquet(self.data_path['blocks'])
            .select(pl.col('number').alias('block_number'), pl.from_epoch(pl.col('timestamp')))
        )
        q = txs.join(blocks, left_on='block_number',
                     right_on='block_number', how='left')

        return q.collect(streaming=True)

    def get_receipts(self, wallet_addresses):
        q = (pl.scan_parquet(self.data_path['tx_receipts'])
             # .filter(pl.col('from').eq(pl.col('to')))
             # .filter(pl.col('from').str.to_lowercase().is_in(wallet_addresses) | pl.col('to').str.to_lowercase().is_in(wallet_addresses))
             .filter(pl.col('from').str.to_lowercase().is_in(wallet_addresses))
             .select([
                 pl.col('transactionHash').alias('tx_hash'),
                 pl.col('gasUsed').alias('gas_used'),
                 pl.col('effectiveGasPrice').alias('gas_effective_price'),
                 pl.col('gasUsed').mul(
                     pl.col('effectiveGasPrice')).truediv(1e18).alias('fees'),
                 pl.col('status').alias('tx_status'),
             ])
             )
        return q.collect(streaming=True)

    def get_event_name(self, signature):
        # https://www.4byte.directory/event-signatures/
        if signature in self.events_dict:
            return self.events_dict[signature]['name']
        return 'Unknown'

    def get_event_signature(self, signature):
        if signature in self.events_dict:
            return self.events_dict[signature]['signature']
        return 'Unknown'

    def get_min_max_blocks(self):
        # get the min and max block number
        q = (
            pl.scan_parquet(self.data_path['blocks'])
            .filter(pl.col('number') > 0)
            .select([pl.col('number').min().alias('min_number'),
                    pl.col('number').max().alias('max_number'),
                    pl.from_epoch(pl.col('timestamp').min()
                                  ).alias('min_timestamp'),
                    pl.from_epoch(pl.col('timestamp').max()).alias('max_timestamp')])
        )
        return q.collect(streaming=True)

    def get_num_transactions(self):
        q = (
            pl.scan_parquet(self.data_path['transactions'])
            .select(pl.len())
        )
        return q.collect(streaming=True).rows()[0][0]

    def get_num_blocks(self):
        q = (
            pl.scan_parquet(self.data_path['blocks'])
            .select(pl.col('hash').n_unique())
        )
        return q.collect(streaming=True).rows()[0][0]

    def get_events_from_contract_address(self, contract_address):
        q = (
            pl.scan_parquet(self.data_path['logs'])
            .filter(pl.col('address').str.to_lowercase() == contract_address.lower())
            .select(pl.col('topics_0').unique().alias('topics_0'))
        )
        return q.collect(streaming=True)

    def get_topics_0_count(self, contract_address):
        q = (
            pl.scan_parquet(self.data_path['logs'])
            .filter(pl.col('address').str.to_lowercase() == contract_address)
            .group_by(pl.col('topics_0'))
            .agg(pl.len())
            .sort(pl.col('len'), descending=True)
        )
        return q.collect(streaming=True)

    def get_count_unique_transactions_per_contract(self, contract_address):
        response = dict()
        response['from'] = (
            pl.scan_parquet(self.data_path['transactions'])
            .filter(pl.col('from').str.to_lowercase() == contract_address)
            .select(pl.col('hash').n_unique())
        ).collect(streaming=True)['hash'][0]
        response['to'] = (
            pl.scan_parquet(self.data_path['transactions'])
            .filter(pl.col('to').str.to_lowercase() == contract_address)
            .select(pl.col('hash').n_unique())
        ).collect(streaming=True)['hash'][0]

        response['from_to'] = (
            pl.scan_parquet(self.data_path['transactions'])
            .filter((pl.col('from').str.to_lowercase() == contract_address) | (pl.col('to').str.to_lowercase() == contract_address))
            .select(pl.col('hash').n_unique())
        ).collect(streaming=True)['hash'][0]
        return response

    def get_unique_transactions_calling_contract(self, contract_address):
        q = (
            pl.scan_parquet(self.data_path['logs'])
            .filter(pl.col('address').str.to_lowercase() == contract_address)
            .select(pl.col('transactionHash').unique())
        )
        return q.collect(streaming=True)

    def get_contract_events(self, contract_address):
        q = (
            pl.scan_parquet(self.data_path['logs'])
            .filter(pl.col('address').str.to_lowercase() == contract_address)
            .select(pl.col(
                ['blockNumber', 'transactionHash', 'transactionIndex', 'logIndex',
                 'topics_0', 'topics_1', 'topics_2', 'topics_3',
                 'data']
            ))
            .sort(pl.col(['blockNumber', 'transactionIndex', 'logIndex']))
        )
        return q.collect(streaming=True)

    def get_contract_transfer_events_bkp(self, contract_address):
        q = (
            pl.scan_parquet(self.data_path['logs'])
            .filter(
                (pl.col('address').str.to_lowercase() == contract_address)
                &
                (pl.col('topics_0').eq("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")))
            .select([
                pl.col('blockNumber'),
                pl.col('transactionHash'),
                pl.col('transactionIndex'),
                pl.col('logIndex'),
                pl.format("0x{}", pl.col(
                    'topics_1').str.slice(-40)).alias('sender'),
                pl.format("0x{}", pl.col('topics_2').str.slice(-40)
                          ).alias('receiver'),
                pl.col('data').str.replace('0x', '0x0').alias('amount')
            ])
        )
        return q.collect(streaming=True)

    def get_contract_transfer_events(self, contract_address):
        txs = (
            pl.scan_parquet(self.data_path['logs'])
            .filter(
                (pl.col('address').str.to_lowercase() == contract_address)
                &
                (pl.col('topics_0').eq("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")))
            .select([
                pl.col('blockNumber'),
                pl.col('transactionHash'),
                pl.col('transactionIndex'),
                pl.col('logIndex'),
                pl.format("0x{}", pl.col(
                    'topics_1').str.slice(-40)).alias('sender'),
                pl.format("0x{}", pl.col('topics_2').str.slice(-40)
                          ).alias('receiver'),
                pl.col('data').str.replace('0x', '0x0').alias('amount')
            ])
        )
        blocks = (
            pl.scan_parquet(self.data_path['blocks'])
            .select(pl.col('number'), pl.from_epoch(pl.col('timestamp')))
        )
        q = txs.join(blocks, left_on='blockNumber', right_on='number', how='left').sort(
            pl.col(['blockNumber', 'transactionIndex', 'logIndex']))
        return q.collect(streaming=True)

    def get_events_from_transactions(self, transactions):
        q = (
            pl.scan_parquet(self.data_path['logs'])
            .filter(pl.col('transactionHash').is_in(transactions['transactionHash']))
            .select(pl.col(
                ['blockNumber', 'transactionHash', 'transactionIndex',
                 'logIndex', 'topics_0', 'topics_1', 'topics_2', 'topics_3',
                 'data']
            ))
            .sort(pl.col(['blockNumber', 'transactionIndex', 'logIndex']))
        )
        return q.collect(streaming=True)

    def get_contract_calls(self, contract_address):
        contract_address = contract_address.lower()
        q = (
            pl.scan_parquet(self.data_path['logs'])
            .filter(
                (pl.col('topics_1').map_elements(Utils.parse_addresses) == contract_address) | (
                    pl.col('topics_2').map_elements(Utils.parse_addresses) == contract_address))
            .select(pl.col(
                ['blockNumber', 'transactionHash', 'transactionIndex',
                 'logIndex', 'address',
                 'topics_0', 'topics_1', 'topics_2', 'topics_3',
                 'data']
            ))
            .sort(pl.col(['blockNumber', 'transactionIndex', 'logIndex']))
        )
        return q.collect(streaming=True)

    def get_fees_spent(self, user_address):
        txs = (
            pl.scan_parquet(self.data_path['tx_receipts'])
            .filter(pl.col('from').str.to_lowercase() == user_address)
            .select(pl.col('blockNumber'), pl.col('gasUsed'),
                    pl.col('effectiveGasPrice'),
                    pl.col('gasUsed').mul(pl.col('effectiveGasPrice')).alias('fees'))
        ).collect(streaming=True)
        min_block = txs['blockNumber'].min()
        max_block = txs['blockNumber'].max()
        blocks = (
            pl.scan_parquet(self.data_path['blocks'])
            .filter((pl.col('number') >= min_block) & (pl.col('number') <= max_block))
            .select(pl.col('number'), pl.from_epoch(pl.col('timestamp')))
        ).collect(streaming=True)
        return txs.join(blocks, left_on='blockNumber', right_on='number', how='left')

    def get_fees_spent_by_contract(self, contract_address):
        txs = (
            pl.scan_parquet(self.data_path['tx_receipts'])
            .filter(pl.col('from').str.to_lowercase() == contract_address)
            .select(pl.col('blockNumber'), pl.col('gasUsed'),
                    pl.col('effectiveGasPrice'),
                    pl.col('gasUsed').mul(pl.col('effectiveGasPrice')).alias('fees'))
        ).collect(streaming=True)
        min_block = txs['blockNumber'].min()
        max_block = txs['blockNumber'].max()
        blocks = (
            pl.scan_parquet(self.data_path['blocks'])
            .filter((pl.col('number') >= min_block) & (pl.col('number') <= max_block))
            .select(pl.col('number'), pl.from_epoch(pl.col('timestamp')))
        ).collect(streaming=True)
        return txs.join(blocks, left_on='blockNumber', right_on='number', how='left')

    def compute_account_balances(self, transfer_df):
        # Loading account balances history for specific addresses
        balances_history_dict = dict()
        for row in tqdm(transfer_df.iter_rows(named=True), desc='Loading balances', total=transfer_df.shape[0]):
            if row['sender'] not in balances_history_dict:
                balances_history_dict[row['sender']] = {
                    'current': 0, 'history': [], 'n_sender': 0, 'n_receiver': 0}
            if row['receiver'] not in balances_history_dict:
                balances_history_dict[row['receiver']] = {
                    'current': 0, 'history': [], 'n_sender': 0, 'n_receiver': 0}

            balance = row['amount']/1e18

            balances_history_dict[row['sender']]['history'].append(
                {'block_number': row['blockNumber'], 'timestamp': row['timestamp'],
                 'balance': balances_history_dict[row['sender']]['current']-balance})
            balances_history_dict[row['receiver']]['history'].append(
                {'block_number': row['blockNumber'], 'timestamp': row['timestamp'],
                 'balance': balances_history_dict[row['receiver']]['current']+balance})

            balances_history_dict[row['sender']]['n_sender'] += 1
            balances_history_dict[row['receiver']]['n_receiver'] += 1

            balances_history_dict[row['sender']]['current'] -= balance
            balances_history_dict[row['receiver']]['current'] += balance

        print("There are in total {} addresses".format(
            len(balances_history_dict)))
        return balances_history_dict

    def count_occurrences(self, file, column):
        q = (
            pl.scan_parquet(self.data_path[file])
            .group_by(pl.col(column).str.to_lowercase())
            .agg(pl.len())
            .sort(pl.col('len'), descending=True)
        )
        return q.collect(streaming=True)

    def get_total_txs_per_address(self):
        q = (
            pl.scan_parquet(self.data_path['transactions'])
            .group_by('from')
            .agg(pl.len('from').alias('n_txs'))
            .select([pl.col('from').str.to_lowercase().alias('address'), pl.col('n_txs')])
            .sort('n_txs', descending=True)
        )
        return q.collect(streaming=True)

    def get_total_transactions_per_day(self):
        q_1 = (pl.scan_parquet(self.data_path['transactions'])
               # .filter(pl.col('blockNumber').is_between(min_block, max_block))
               .select('blockNumber')
               )
        q_2 = (pl.scan_parquet(self.data_path['blocks'])
               # .filter(pl.col('number').is_between(min_block, max_block))
               .select(pl.col('number'), pl.from_epoch(pl.col('timestamp')))
               )
        q = q_1.join(q_2, left_on='blockNumber', right_on='number', how='left')
        q = (q
             .group_by(pl.col('timestamp').cast(pl.Date).alias('date'))
             .agg(pl.len())
             .sort(pl.col('date'))
             )
        return q.collect(streaming=True)

    # def get_transactions_per_day_per_contract(contract_address):
    #     q_1 = (pl.scan_parquet(data_path['transactions'])
    #            .filter(pl.col('to').str.to_lowercase() == contract_address)
    #            .select('blockNumber')
    #            )
    #     q_2 = (pl.scan_parquet(data_path['blocks'])
    #            .select(pl.col('number'), pl.from_epoch(pl.col('timestamp')))
    #            )
    #     q = q_1.join(q_2, left_on='blockNumber', right_on='number', how='left')
    #     q = (q
    #          .group_by(pl.col('timestamp').cast(pl.Date).alias('date'))
    #          .agg(pl.len())
    #          .sort(pl.col('date'))
    #          )
    #     return q.collect(streaming=True)

    def get_transactions_per_day_per_contract(self, contract_addresses):
        q_1 = (pl.scan_parquet(self.data_path['transactions'])
               .filter(pl.col('to').str.to_lowercase().is_in(contract_addresses))
               .select(pl.col('blockNumber'), pl.col('to').alias('contractAddress').str.to_lowercase())
               )
        q_2 = (pl.scan_parquet(self.data_path['blocks'])
               .select(pl.col('number'), pl.col('timestamp'))
               )
        q = q_1.join(q_2, left_on='blockNumber', right_on='number', how='left')
        q = (q
             .group_by([pl.col('contractAddress'), pl.from_epoch(pl.col('timestamp')).cast(pl.Date).alias('date')])
             .agg(pl.len())
             .sort(pl.col('date'))
             )
        return q.collect(streaming=True)


def construct_events_dict(self):
    self.events_dict = dict()
    self.events_dict['0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'] = {
        'name': 'Transfer', 'signature': 'Transfer(address,address,uint256)'}
    self.events_dict['0x7f26b83ff96e1f2b6a682f133852f6798a09c465da95921460cefb3847402498'] = {
        'name': 'Initialized', 'signature': 'Initialized(uint8)'}
    self.events_dict['0x9b5b9a05e4726d8bb959f1440e05c6b8109443f2083bc4e386237d7654526553'] = {
        'name': 'BridgeBurn', 'signature': 'BridgeBurn(address,uint256)'}
    self.events_dict['0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925'] = {
        'name': 'Approval', 'signature': 'Approval(address,address,uint256)'}
    self.events_dict['0x1cf3b03a6cf19fa2baba4df148e9dcabedea7f8a5c07840e207e5c089be95d3e'] = {
        'name': 'BeaconUpgraded', 'signature': 'BeaconUpgraded(address)'}
    self.events_dict['0x397b33b307fc137878ebfc75b295289ec0ee25a31bb5bf034f33256fe8ea2aa6'] = {
        'name': 'BridgeMint', 'signature': 'BridgeMint(address,uint256)'}
    self.events_dict['0xe6b2ac4004ee4493db8844da5db69722d2128345671818c3c41928655a83fb2c'] = {
        'name': 'Unknown', 'signature': 'Unknown'}
    self.events_dict['0x66753cd2356569ee081232e3be8909b950e0a76c1f8460c3a5e3c2be32b11bed'] = {
        'name': 'SafeMultiSigTransaction', 'signature': 'SafeMultiSigTransaction(address,uint256,bytes,uint8,uint256,uint256,uint256,address,address,bytes,bytes)'
    }
    self.events_dict['0x25bc54a32c894b07fd47ed3cc4296ec7d97a974e5ebd17c9f5163afddaf107fa'] = {
        'name': 'PairCreated', 'signature': 'PairCreated(address,address,bool,address,uint256,uint256)'
    }
    self.events_dict['0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9'] = {
        'name': 'PairCreated', 'signature': 'PairCreated(address,address,address,uint256)'
    }
    self.events_dict['0x9c5d829b9b23efc461f9aeef91979ec04bb903feb3bee4f26d22114abfc7335b'] = {
        'name': 'PoolCreated', 'signature': 'PoolCreated(address,address,address)'
    }
    self.events_dict['0xe5b6779c4a18cbf7e4bce3a6c308b215c678f316648b832318a03841664fc2e9'] = {
        'name': 'Add', 'signature': 'Add(uint256,address,uint256)'
    }
    self.events_dict['0x90890809c654f11d6e72a28fa60149770a0d11ec6c92319d6ceb2bb0a4ea1a15'] = {
        'name': 'Deposit', 'signature': 'Deposit(address,uint256,uint256)'
    }
    self.events_dict['0x99d2b755eb38920131acb332adf086ea38d15009f223c21f3aa978d6ab234786'] = {
        'name': 'TokenListed', 'signature': 'TokenListed(address,address)'
    }
    self.events_dict['0x76af224a143865a50b41496e1a73622698692c565c1214bc862f18e22d829c5e'] = {
        'name': 'Swapped', 'signature': 'Swapped(address,address,address,address,uint256,uint256,uint256,uint256,uint256,address)'

    }
    self.events_dict['0x0fda88444c12e1f4744fdef52d79861522a79baa20a440385e449e39f0de5cd5'] = {
        'name': 'PoolAmountUpdated', 'signature': 'PoolAmountUpdated(address,uint256,uint256,uint256,uint256)'
    }
    self.events_dict['0x4daa66261ce0fd318a8b8e5a12526b9e43f3f411a9cb6488e3959d6d12313787'] = {
        'name': 'CaptureSwapFee', 'signature': 'CaptureSwapFee(address,uint256,uint256)'
    }
    self.events_dict['0x5b99554095b82fa9d0725a9ff1f1db8bc3e4d461e7d61911c752c349a47b987f'] = {
        'name': 'Executed', 'signature': 'Executed(address,uint256,uint256,address,uint256,uint256,uint256,uint256,(uint8,(uint256,address),(uint256,address),address,address,bytes),address)'

    }
    self.events_dict['0x4bc8151c051441255339d01fbaeb38cf109cbfd75e9a5c62fb8f1dfb37fe6fd6'] = {
        'name': 'Unknown', 'signature': 'Unknown'

    }
    self.events_dict['0x76e3540b214147a4d1733ed21adbc36bc76fe68060033ed6a6dc81a8d3e699a4'] = {
        'name': 'AddLiquidity', 'signature': 'AddLiquidity(address,uint256,uint256,address)'
    }
    self.events_dict['0x7055e3d08e2c20429c6b162f3e3bee3f426d59896e66084c3580dc353e54129d'] = {
        'name': 'TransitSwapped', 'signature': 'TransitSwapped(address,address,address,address,bool,uint256,uint256,uint256,uint256,uint256,string,uint256)'
    }
    self.events_dict['0x3d58404efba0e9fa8c42b15fd0c0aee3cc2ac3a59477f4e392bff292d4977879'] = {
        'name': 'RemoveLiquidity', 'signature': 'RemoveLiquidity(address,uint256,uint256,address)'

    }
    self.events_dict['0xdec2bacdd2f05b59de34da9b523dff8be42e5e38e818c82fdb0bae774387a724'] = {
        'name': 'DelegateVotesChanged', 'signature': 'DelegateVotesChanged(address,uint256,uint256)'

    }
    self.events_dict['0xb5cb27a38d4490736679c8feb25b220de6e56af98bbc4d61378dffde8e46101c'] = {
        'name': 'Unknown', 'signature': 'Unknown'

    }

    self.events_dict['0x9aa05b3d70a9e3e2f004f039648839560576334fb45c81f91b6db03ad9e2efc9'] = {
        'name': 'ClaimRewards', 'signature': 'ClaimRewards(address,address,uint256)'

    }

    self.events_dict['0x3134e8a2e6d97e929a7e54011ea5485d7d196dd5f0ba4d4ef95803e8e3fc257f'] = {
        'name': 'DelegateChanged', 'signature': 'DelegateChanged(address,address,address)'

    }

    self.events_dict['0xf70d5c697de7ea828df48e5c4573cb2194c659f1901f70110c52b066dcf50826'] = {
        'name': 'NotifyReward', 'signature': 'NotifyReward(address,address,uint256)'

    }

    self.events_dict['0x0d7d75e01ab95780d3cd1c8ec0dd6c2ce19e3a20427eec8bf53283b6fb8e95f0'] = {
        'name': 'FlashLoan', 'signature': 'FlashLoan(address,address,uint256,uint256)'

    }

    self.events_dict['0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118'] = {
        'name': 'PoolCreated', 'signature': 'PoolCreated(address,address,uint24,int24,address)'

    }
    self.events_dict['0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822'] = {
        'name': 'Swap', 'signature': 'Swap(address,uint256,uint256,uint256,uint256,address)'

    }

    self.events_dict['0xcf2aa50876cdfbb541206f89af0ee78d44a2abf8d328e37fa4917f982149848a'] = {
        'name': 'Sync', 'signature': 'Sync(uint256,uint256)'

    }

    self.events_dict['0x0f6798a560793a54c3bcfe86a93cde1e73087d944c0ea20544137d4121396885'] = {
        'name': 'Mint', 'signature': 'Mint(address,uint256)'

    }
    self.events_dict['0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1'] = {
        'name': 'Sync', 'signature': 'Sync(uint112,uint112)'

    }

    self.events_dict['0xe7779a36a28ae0e49bcbd9fcf57286fb607699c0c339c202e92495640505613e'] = {
        'name': 'Swap', 'signature': 'Swap(address,address,uint24,bool,uint256,uint256)'

    }

    self.events_dict['0x3b841dc9ab51e3104bda4f61b41e4271192d22cd19da5ee6e292dc8e2744f713'] = {
        'name': 'Swap', 'signature': 'Swap(address,address,bool,bool,uint256,uint256,int32)'

    }

    self.events_dict['0xf26bfd49b39c52efaf04ee7f21ca2fdc73c680fada92ab7a8f1ea37b350bcf8c'] = {
        'name': 'Message', 'signature': 'Message(string,address,string)'

    }

    self.events_dict['0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f'] = {
        'name': 'Mint', 'signature': 'Mint(address,uint256,uint256)'

    }

    self.events_dict['0x2caecd17d02f56fa897705dcc740da2d237c373f70686f4e0d9bd3bf0400ea7a'] = {
        'name': 'DistributedSupplierComp', 'signature': 'DistributedSupplierComp(address,address,uint256,uint256)'

    }

    self.events_dict['0xce0457fe73731f824cc272376169235128c118b49d344817417c6d108d155e82'] = {
        'name': 'NewOwner', 'signature': 'NewOwner(bytes32,bytes32,address)'

    }

    self.events_dict['0xa8137fff86647d8a402117b9c5dbda627f721d3773338fb9678c83e54ed39080'] = {
        'name': 'Mint', 'signature': 'Mint(address,uint256,uint256,uint256,address)'

    }

    self.events_dict['0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62'] = {
        'name': 'TransferSingle', 'signature': 'TransferSingle(address,address,address,uint256,uint256)'

    }

    self.events_dict['0xe5b754fb1abb7f01b499791d0b820ae3b6af3424ac1c59768edb53f4ec31a929'] = {
        'name': 'Redeem', 'signature': 'Redeem(address,uint256,uint256)'

    }

    self.events_dict['0xfa76a4010d9533e3e964f2930a65fb6042a12fa6ff5b08281837a10b0be7321e'] = {
        'name': 'TokensClaimed', 'signature': 'TokensClaimed(uint256,address,address,uint256,uint256)'

    }

    self.events_dict['0xbc7cd75a20ee27fd9adebab32041f755214dbc6bffa90cc0225b39da2e5c2d3b'] = {
        'name': 'Upgraded', 'signature': 'Upgraded(address)'
    }

    self.events_dict['0x1271330ebae535a48b1e51c35100aab270c68fe44a98e2c04ce905979a28ffbf'] = {
        'name': 'ProtocolFeeDynamicChange', 'signature': 'ProtocolFeeDynamicChange(uint256)'
    }

    self.events_dict['0x11d0eb59e2eba9f45cb018cb9a3009b3646d176071d40521ad22cdbc1c8170bc'] = {
        'name': 'ProtocolFeeToChange', 'signature': 'ProtocolFeeToChange(address)'
    }

    self.events_dict['0x7e644d79422f17c01e4894b5f4f588d331ebfa28653d42ae832dc59e38c9798f'] = {
        'name': 'AdminChanged', 'signature': 'AdminChanged(address,address)'
    }

    self.events_dict['0xc21caeb4e8f73861400d4c0870ad3e468ddb4e45225da3832ce1da5561f1f61e'] = {
        'name': 'Unknown', 'signature': 'Unknown'
    }

    self.events_dict['0x865ca08d59f5cb456e85cd2f7ef63664ea4f73327414e9d8152c4158b0e94645'] = {
        'name': 'Claim', 'signature': 'Claim(address,address,uint256,uint256)'
    }

    self.events_dict['0xebf2df875b555f5edaef342e52b6a9498cccaec4813df8eb0f3842acc6d08281'] = {
        'name': 'Fees', 'signature': 'Fees(address,uint256,uint256,uint256,uint256)'
    }

    self.events_dict['0xdccd412f0b1252819cb1fd330b93224ca42612892bb3f4f789976e6d81936496'] = {
        'name': 'Burn', 'signature': 'Burn(address,uint256,uint256,address)'
    }

    self.events_dict['0x91e72fa36e0202be93e86c97a3d3d3497cf0a06cf859b14b616a304367835a8e'] = {
        'name': 'ChangeFee', 'signature': 'ChangeFee(uint256,uint256)'
    }
    self.events_dict['0x6eef835a3905d9ada7e0cc03c9912f07fac2fe4c8f1b503a0b31a93b55c5ca18'] = {
        'name': 'PriceRequested', 'signature': 'PriceRequested(uint256,bytes32,uint256,uint8,uint256,uint256)'
    }

    self.events_dict['0x3e544118c04e3bb18b669475695cd270ba0e41fb13177483f01c14222de62a86'] = {
        'name': 'MarketOrderInitiated', 'signature': 'MarketOrderInitiated(uint256,address,uint256,bool)'
    }

    self.events_dict['0x2dc8e290002f06fc0085bbca9dfb8b415cf4d1178950c72ff9ee8f4d8878ee66'] = {
        'name': 'Refunded', 'signature': 'Refunded(address,uint256,uint256)'
    }

    self.events_dict['0x606834f57405380c4fb88d1f4850326ad3885f014bab3b568dfbf7a041eef738'] = {
        'name': 'Received', 'signature': 'Received(uint256,address,bytes)'
    }

    self.events_dict['0x31c5374dcc95d137f3c21741a151157300dc87c02ffd59e4a177713557a916b1'] = {
        'name': 'ImplementationUpdated', 'signature': 'ImplementationUpdated(address,string,address)'
    }

    self.events_dict['0xfd2dd1404be8946179e2efaadd5b7b78920403c755422016c708b3c84d5fd8e3'] = {
        'name': 'Unknown', 'signature': 'Unknown'
    }

    self.events_dict['0x2a2899ae8177fb54d81544d727d24141bf8fd23e78e721fe56cda337d397044b'] = {
        'name': 'Unknown', 'signature': 'Unknown'
    }
    self.events_dict['0xbaec78ca3218aba6fc32d82b79acdd1a47663d7b8da46e0c00947206d08f2071'] = {
        'name': 'Swap', 'signature': 'Swap(address,address,bytes32[],int128[])'
    }

    self.events_dict['0x19b47279256b2a23a1665c810c8d55a1758940ee09377d4f8d26497a3577dc83'] = {
        'name': 'Swap', 'signature': 'Swap(address,address,int256,int256,uint160,uint128,int24,uint128,uint128)'
    }

    self.events_dict['0x4dec04e750ca11537cabcd8a9eab06494de08da3735bc8871cd41250e190bc04'] = {
        'name': 'AccrueInterest', 'signature': 'AccrueInterest(uint256,uint256,uint256,uint256)'
    }

    self.events_dict['0xa3e4886b89c6d25cb1409eb38693c679fbc0122c8f524f71c8b7c0ea4fde21a5'] = {
        'name': 'GetTicket', 'signature': 'GetTicket(address,uint256)'
    }

    self.events_dict['0xd175a80c109434bb89948928ab2475a6647c94244cb70002197896423c883363'] = {
        'name': 'Burn', 'signature': 'Burn(address,uint256,uint256,uint256,address)'
    }

    self.events_dict['0x290afdae231a3fc0bbae8b1af63698b0a1d79b21ad17df0342dfb952fe74f8e5'] = {
        'name': 'ContractDeployed', 'signature': 'ContractDeployed(address,bytes32,address)'
    }

    self.events_dict['0x14be1c122ee505f1d86e447cb0ae7bf79456d3f660501d9d24f901e3bbc8e65c'] = {
        'name': 'MintSoul', 'signature': 'MintSoul(address,uint256,uint256)'
    }
    self.events_dict['0xe9bded5f24a4168e4f3bf44e00298c993b22376aad8c58c7dda9718a54cbea82'] = {
        'name': 'Packet', 'signature': 'Packet(bytes)'
    }

    self.events_dict['0xdf21c415b78ed2552cc9971249e32a053abce6087a0ae0fbf3f78db5174a3493'] = {
        'name': 'AssignJob', 'signature': 'AssignJob(uint256)'
    }

    self.events_dict['0xb0c632f55f1e1b3b2c3d82f41ee4716bb4c00f0f5d84cdafc141581bb8757a4f'] = {
        'name': 'RelayerParams', 'signature': 'RelayerParams(bytes,uint16)'
    }

    self.events_dict['0x4e41ee13e03cd5e0446487b524fdc48af6acf26c074dacdbdfb6b574b42c8146'] = {
        'name': 'AssignJob', 'signature': 'AssignJob(uint16,uint16,uint256,address,uint256)'
    }

    self.events_dict['0x9c248aa1a265aa616f707b979d57f4529bb63a4fc34dc7fc61fdddc18410f74e'] = {
        'name': 'ERC721SellOrderFilled', 'signature': 'ERC721SellOrderFilled(bytes32,address,address,uint256,address,uint256,(address,uint256)[],address,uint256)'
    }
    self.events_dict['0x52977ea98a2220a03ee9ba5cb003ada08d394ea10155483c95dc2dc77a7eb24b'] = {
        'name': 'NotifyReward', 'signature': 'NotifyReward(address,address,uint256,uint256)'
    }

    self.events_dict['0x3ab23ab0d51cccc0c3085aec51f99228625aa1a922b3a8ca89a26b0f2027a1a5'] = {
        'name': 'MarketEntered', 'signature': 'MarketEntered(address,address)'
    }

    self.events_dict['0x3a36e47291f4201faf137fab081d92295bce2d53be2c6ca68ba82c7faa9ce241'] = {
        'name': 'L1MessageSent', 'signature': 'L1MessageSent(address,bytes32,bytes)'
    }

    self.events_dict['0x17307eab39ab6107e8899845ad3d59bd9653f200f220920489ca2b5937696c31'] = {
        'name': 'ApprovalForAll', 'signature': 'ApprovalForAll(address,address,bool)'
    }

    self.events_dict['0x0e8e403c2d36126272b08c75823e988381d9dc47f2f0a9a080d95f891d95c469'] = {
        'name': 'WooSwap', 'signature': 'WooSwap(address,address,uint256,uint256,address,address,address,uint256,uint256)'
    }

    self.events_dict['0x2717ead6b9200dd235aad468c9809ea400fe33ac69b5bfaa6d3e90fc922b6398'] = {
        'name': 'Withdrawal', 'signature': 'Withdrawal(address,address,uint256)'
    }

    self.events_dict['0x3303facd24627943a92e9dc87cfbb34b15c49b726eec3ad3487c16be9ab8efe8'] = {
        'name': 'Accrued', 'signature': 'Accrued(address,address,address,uint256,uint256,uint256)'
    }

    self.events_dict['0x112c256902bf554b6ed882d2936687aaeb4225e8cd5b51303c90ca6cf43a8602'] = {
        'name': 'Fees', 'signature': 'Fees(address,uint256,uint256)'
    }

    self.events_dict['0x27c98e911efdd224f4002f6cd831c3ad0d2759ee176f9ee8466d95826af22a1c'] = {
        'name': 'WooRouterSwap', 'signature': 'WooRouterSwap(uint8,address,address,uint256,uint256,address,address,address)'
    }

    self.events_dict['0x69ca02dd4edd7bf0a4abb9ed3b7af3f14778db5d61921c7dc7cd545266326de2'] = {
        'name': 'Transfer', 'signature': 'Transfer(address,uint256)'
    }

    self.events_dict['0x055a181b27c0ef897e8c559755721e45a372d5ac946a2ae3905b8a4364e8745b'] = {
        'name': 'EventClaim', 'signature': 'EventClaim(uint256,uint256,uint256,address,address)'
    }

    self.events_dict['0xddac40937f35385a34f721af292e5a83fc5b840f722bff57c2fc71adba708c48'] = {
        'name': 'Exchange', 'signature': 'Exchange(address,uint256,address)'
    }

    self.events_dict['0x1fc3ecc087d8d2d15e23d0032af5a47059c3892d003d8e139fdcb6bb327c99a6'] = {
        'name': 'DistributedBorrowerComp', 'signature': 'DistributedBorrowerComp(address,address,uint256,uint256)'
    }

    self.events_dict['0xb3d987963d01b2f68493b4bdb130988f157ea43070d4ad840fee0466ed9370d9'] = {
        'name': 'NameRegistered', 'signature': 'NameRegistered(uint256,address,uint256)'
    }

    self.events_dict['0xca6abbe9d7f11422cb6ca7629fbf6fe9efb1c621f71ce8f02b9f2a230097404f'] = {
        'name': 'NameRegistered', 'signature': 'NameRegistered(string,bytes32,address,uint256,uint256)'
    }

    self.events_dict['0x2f00e3cdd69a77be7ed215ec7b2a36784dd158f921fca79ac29deffa353fe6ee'] = {
        'name': 'Mint', 'signature': 'Mint(address,address,uint256,uint256)'
    }

    self.events_dict['0x1eeaa4acf3c225a4033105c2647625dbb298dec93b14e16253c4231e26c02b1d'] = {
        'name': 'Swapped', 'signature': 'Swapped(address,address,address,uint256,uint256,address)'
    }

    self.events_dict['0x47cee97cb7acd717b3c0aa1435d004cd5b3c8c57d70dbceb4e4458bbd60e39d4'] = {
        'name': 'Claim', 'signature': 'Claim(address,uint256)'
    }

    self.events_dict['0x3f693fff038bb8a046aa76d9516190ac7444f7d69cf952c4cbdc086fdef2d6fc'] = {
        'name': 'Redeem', 'signature': 'Redeem(address,address,uint256,uint256)'
    }
