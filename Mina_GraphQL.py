from MinaGraphQL.MinaClient import Client
import logging 
import asyncio
import configparser
import json
import time
import warnings
import psycopg2
import pandas as pd
import pandas.io.sql as sqlio
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
warnings.filterwarnings("ignore")

class MinaGraphQL:
    '''Mina GraphQL Script for subscribing to the GraphQL and adding new blocks'''
    def __init__( self, config_file='config.ini' ):
        # file names
        self.files = {  'config': config_file }

        # read config file
        self.config = self.read_config( )

        # set mode
        self.mode = 'nominal'

        # set logger basic config
        if self.mode in [ 'debug', 'test' ]:
            log_level = logging.DEBUG
        else:
            log_level = logging.INFO
        logging.basicConfig( filename = 'output.log',
                    format = '%(asctime)s.%(msecs)03d %(levelname)s: %(message)s',
                    level = log_level )
        self.logger = logging.getLogger(__name__)

        # connect to database
        self.database = self.connect_db( self.config[ 'Mainnet' ] )
        self.cursor = self.database.cursor()

        # get the client
        self.client = Client( graphql_host=self.config['GraphQL']['host'], graphql_port=self.config['GraphQL']['port'] )

        # get the sync status
        sync_status = self.client.get_sync_status()['syncStatus']
        self.logger.info( f"Sync Status {sync_status}..." )
        while sync_status != 'SYNCED':
            self.logger.info( f"Sync Status {sync_status} - Sleeping for 5 Seconds" )
            time.sleep( 5 )
            sync_status = self.client.get_sync_status()['syncStatus']
        
        self.logger.info( f"Sync Status {sync_status}! Running async runner for GraphQL!" )

        # run the block listener        
        asyncio.run(self.client.listen_new_blocks( self.parse_data ))

    def read_config( self ):
        '''read the config file'''
        config = configparser.ConfigParser(interpolation=None)
        config.read( self.files[ 'config' ] )
        return config

    def connect_db( self, info ):
        '''establish the postgres'''
        self.logger.info( f"Connecting to Database {info[ 'database' ]} at {info[ 'host' ]}:{info[ 'port' ]}")
        # connect
        conn = psycopg2.connect(
            database =  info[ 'database' ],
            user =      info[ 'user' ],
            password =  info[ 'password' ],
            host =      info[ 'host' ],
            port =      info[ 'port' ] )
        # set isolation level
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        self.logger.info( f"Connected to Database {info[ 'database' ]}!")
        return conn

    def parse_data( self, data ) :
        '''parses the return data in the callback'''
        # parse if it is data it is a new block
        data = json.loads( data )
        payload_keys = data['payload'].keys()
        if data['type'] == 'data':
            if 'data' in payload_keys:
                self.parse_block( data['payload']['data']['newBlock'] )
            elif 'ChainReorganization' in payload_keys:
                self.logger.info( f"Reorganization: { data['payload']}" )

    def parse_block( self, data ):
        '''Parses the block from the graphql'''
        # check for current best tip
        best_chain = self.parse_best_chain( self.client.get_best_chain( 290 ) )

        # populate the fields
        creator =               data['creatorAccount']['publicKey']
        block_hash =            data['stateHash']
        parent_hash =           data['protocolState']['previousStateHash']
        timestamp =             data['protocolState']['blockchainState']['date']
        snarked_ledger_hash =   data['protocolState']['blockchainState']['snarkedLedgerHash']
        staged_ledger_hash =    data['protocolState']['blockchainState']['stagedLedgerHash']
        block_height =                data['protocolState']['consensusState']['blockHeight']
        epoch =                 data['protocolState']['consensusState']['epoch']
        slot =                  data['protocolState']['consensusState']['slot']
        slot_since_genesis =    data['protocolState']['consensusState']['slotSinceGenesis']
        total_currency =        data['protocolState']['consensusState']['totalCurrency']
        coinbase_receiver =     data['transactions']['coinbaseReceiverAccount']['publicKey']
        coinbase_amount =       data['transactions']['coinbase']
        chain_status =          'pending'
        num_tx =                len( data['transactions']['userCommands'] ) if 'userCommands' in data['transactions'].keys() else 0
        num_zkapp_tx =          len( data['transactions']['zkappCommands'] ) if 'zkappCommands' in data['transactions'].keys() else 0
        num_fee_tx   =          len( data['transactions']['feeTransfer'] ) if 'feeTransfer' in data['transactions'].keys() else 0
        
        # check if it is in the best chain
        if int(block_height) in best_chain.keys():
            if best_chain[int(block_height)] == block_hash:
                chain_status = 'canonical'
            else:
                chain_status = 'orphaned'

        # log the parsing info
        self.logger.info( f"Parsing New Block {block_height} with Hash {block_hash} Created by {creator} [{chain_status}]")

        # build and insert
        block_data = ( block_height, chain_status, creator, block_hash, parent_hash, timestamp, coinbase_amount, coinbase_receiver, snarked_ledger_hash, staged_ledger_hash, epoch, slot, slot_since_genesis, total_currency, num_tx, num_fee_tx, num_zkapp_tx )
        self.insert_block( block_data )

        # insert the creator and coinbase receiver balance
        creator_data = ( creator, data['creatorAccount']['balance']['total'], data['creatorAccount']['balance']['locked'], data['creatorAccount']['balance']['liquid'], block_height, block_hash, chain_status )
        self.insert_balance( creator_data )
        coinbase_data = ( coinbase_receiver, data['transactions']['coinbaseReceiverAccount']['balance']['total'], data['transactions']['coinbaseReceiverAccount']['balance']['locked'], data['transactions']['coinbaseReceiverAccount']['balance']['liquid'], block_height, block_hash, chain_status )
        self.insert_balance( coinbase_data )

        # parse the transactions
        self.parse_transactions( block_height, block_hash, chain_status, data['transactions'] )

        # go back to previous blockheights and change if necessary
        self.check_reorganization( best_chain )

    def parse_transactions( self, block_height, block_hash, chain_status, transactions ):
        '''parses the transactions in the graphql response'''

        balances = {}
        # user transactions
        for tx in transactions['userCommands']:
            sender =    tx['source']['publicKey']
            receiver =  tx['receiver']['publicKey']
            feePayer =  tx['feePayer']['publicKey']
            tx_data = ( block_height, block_hash, tx['hash'], chain_status, tx['kind'], sender, receiver, feePayer, tx['token'], tx['feeToken'], tx['amount'], tx['fee'], tx['memo'], tx['nonce'], tx['failureReason'] )
            self.insert_transaction( tx_data )

            # store the balances
            balances[ sender ] = {  'balance': tx['source']['balance']['total'],
                                    'locked': tx['source']['balance']['locked'],
                                    'liquid': tx['source']['balance']['liquid'] }
            balances[ receiver ] = { 'balance': tx['receiver']['balance']['total'],
                                     'locked': tx['receiver']['balance']['locked'],
                                     'liquid': tx['receiver']['balance']['liquid'] }
            balances[ feePayer ] = { 'balance': tx['feePayer']['balance']['total'],
                                     'locked': tx['feePayer']['balance']['locked'],
                                     'liquid': tx['feePayer']['balance']['liquid'] }

        # fee transactions
        for tx in transactions['feeTransfer']:
            tx_data = ( block_height, block_hash, chain_status, tx['recipient'], tx['fee'] )
            self.insert_fee_transfer( tx_data )

        # update the balances
        for public_key in balances.keys():
            balance_data = ( public_key, balances[public_key]['balance'], balances[public_key]['locked'], balances[public_key]['liquid'], block_height, block_hash, chain_status )
            self.insert_balance( balance_data )        

    def parse_best_chain( self, best_chain ):
        '''parse the best chain'''
        output = dict()
        if 'bestChain' in best_chain.keys():
            for item in best_chain['bestChain']:
                block_height = int( item[ 'protocolState' ][ 'consensusState' ][ 'blockHeight' ] )
                block_hash = item[ 'stateHash' ]
                output[ block_height ] = block_hash
        return output

    def check_reorganization( self, best_chain ):
        '''check for chain reorganizations'''
        for block_height in best_chain.keys():
            block_hash = best_chain[block_height]
            # get the blocks for the block_height
            blocks = self.get_block_state_status( block_height )
            # iterate through all the blocks at the block_height
            for index, block in blocks.iterrows():
                chain_status = block['chain_status']
                # check if both the state and parent hashes are in the best_chain
                if block['block_hash'] == block_hash:
                    # verify it is canonical
                    if chain_status != 'canonical':
                        self.logger.info( f"Updating Block {block_height} {block['block_hash']} from {chain_status} to canonical" )
                        self.update_block_state( block['block_hash'], 'canonical' )
                else:
                    # verify it is orphaned
                    if chain_status != 'orphaned':
                        self.logger.info( f"Updating Block {block_height} {block['block_hash']} from {chain_status} to orphaned" )
                        self.update_block_state( block['block_hash'], 'orphaned' )

    def update_block_state( self, block_hash, chain_status ):
        '''update the chain state for the state and block hash'''
        self.update_block_chain_status( block_hash, chain_status )
        self.update_transaction_chain_status( block_hash, chain_status )
        self.update_fee_transfer_chain_status( block_hash, chain_status )
        self.update_balances_chain_status( block_hash, chain_status )

    def get_df_data( self, db, query ):
        '''query the database'''
        df = pd.DataFrame()
        for chunk in sqlio.read_sql_query( query, db, chunksize=10000 ):
            df = pd.concat([ df, chunk ])
        return df.fillna(0)

    def get_block_state_status( self, block_height ):
        '''for the block_height, get all the block hashs and chain status'''
        query = """SELECT "block_hash","chain_status" FROM blocks
                    WHERE "block_height" = '%s' """ % block_height
        df = self.get_df_data( self.database, query ) 
        return df

    def insert_block( self, data ):
        '''insert the block'''
        self.logger.debug( f'Inserting Block: { data }' )
        cmd = """INSERT INTO blocks (
            block_height,
            chain_status,
            creator,
            block_hash,
            parent_hash,
            timestamp,
            coinbase_amount,
            coinbase_receiver, 
            snarked_ledger_hash, 
            staged_ledger_hash, 
            epoch, 
            slot, 
            global_slot_since_genesis, 
            total_currency, 
            num_transactions, 
            num_fee_transactions, 
            num_zkapp_transactions
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )
            ON CONFLICT DO NOTHING"""
        if self.mode in [ 'nominal', 'test' ]:
            self.cursor.execute( cmd, data )
        else:
            self.logger.debug( f'Block Not Inserted with Mode: {self.mode}' )

    def insert_transaction( self, data ):
            '''insert the transaction'''
            self.logger.debug( f'Inserting Transaction: { data }' )
            cmd = """INSERT INTO transactions (
                block_height,
                block_hash,
                state_hash,
                chain_status,
                kind,
                source,
                receiver,
                fee_payer,
                token,
                fee_token,
                amount,
                fee,
                memo, 
                nonce,
                failure
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING"""
            if self.mode in [ 'nominal', 'test' ]:
                self.cursor.execute( cmd, data )
            else:
                self.logger.debug( f'Transaction Not Inserted with Mode: {self.mode}' )

    def insert_fee_transfer( self, data ):
        '''insert the fee transfer'''
        self.logger.debug( f'Inserting Fee Transfer: { data }' )
        cmd = """INSERT INTO fee_transfers (
            block_height,
            block_hash,
            chain_status,
            receiver,
            amount
            ) VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING"""
        if self.mode in [ 'nominal', 'test' ]:
            self.cursor.execute( cmd, data )
        else:
            self.logger.debug( f'Fee Transfer Not Inserted with Mode: {self.mode}' )

    def insert_balance( self, data ):
        '''insert the balances'''
        self.logger.debug( f'Inserting Balance: { data }' )
        cmd = """INSERT INTO balances (
            public_key,
            balance,
            locked,
            liquid,
            block_height,
            block_hash,
            chain_status
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING"""
        if self.mode in [ 'nominal', 'test' ]:
            self.cursor.execute( cmd, data )
        else:
            self.logger.debug( f'Balance Not Inserted with Mode: {self.mode}' )

    def update_block_chain_status( self, block_hash, chain_status ):
        '''update chain status for the table'''
        self.logger.debug( f"Update the chain status for blocks [ {block_hash}, {chain_status} ]" )

        cmd = """UPDATE blocks
            SET chain_status = %s
            WHERE "block_hash" = %s""" 
        if self.mode in [ 'nominal', 'test' ]:
            self.cursor.execute( cmd, ( chain_status, block_hash ) )
        else:
            self.logger.debug( f'Chain Status Not Updated with Mode: {self.mode}' )

    def update_transaction_chain_status( self, block_hash, chain_status ):
        '''update chain status for the table'''
        self.logger.debug( f"Update the chain status for transactions [ {block_hash}, {chain_status} ]" )

        cmd = """UPDATE transactions
            SET chain_status = %s
            WHERE "block_hash" = %s""" 
        if self.mode in [ 'nominal', 'test' ]:
            self.cursor.execute( cmd, ( chain_status, block_hash ) )
        else:
            self.logger.debug( f'Chain Status Not Updated with Mode: {self.mode}' )

    def update_fee_transfer_chain_status( self, block_hash, chain_status ):
        '''update chain status for the table'''
        self.logger.debug( f"Update the chain status for fee transfer [ {block_hash}, {chain_status} ]" )

        cmd = """UPDATE fee_transfers
            SET chain_status = %s
            WHERE "block_hash" = %s""" 
        if self.mode in [ 'nominal', 'test' ]:
            self.cursor.execute( cmd, ( chain_status, block_hash ) )
        else:
            self.logger.debug( f'Chain Status Not Updated with Mode: {self.mode}' )

    def update_balances_chain_status( self, block_hash, chain_status ):
        '''update chain status for the table'''
        self.logger.debug( f"Update the chain status for balances [ {block_hash}, {chain_status} ]" )

        cmd = """UPDATE balances
            SET chain_status = %s
            WHERE "block_hash" = %s""" 
        if self.mode in [ 'nominal', 'test' ]:
            self.cursor.execute( cmd, ( chain_status, block_hash ) )
        else:
            self.logger.debug( f'Chain Status Not Updated with Mode: {self.mode}' )

# Run GraphQL
graphql = MinaGraphQL( )