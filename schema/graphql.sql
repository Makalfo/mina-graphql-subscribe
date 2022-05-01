CREATE TYPE chain_status_type AS ENUM ('canonical', 'orphaned', 'pending');

CREATE TABLE blocks (
    id SERIAL PRIMARY KEY,
    height INT NOT NULL,
    chain_status chain_status_type NOT NULL,
    creator TEXT NOT NULL,
    block_hash TEXT UNIQUE NOT NULL,
    parent_hash TEXT NOT NULL,
    timestamp BIGINT NOT NULL,
    coinbase_amount BIGINT NOT NULL,
    coinbase_receiver TEXT, 
    snarked_ledger_hash TEXT NOT NULL, 
    staged_ledger_hash TEXT NOT NULL, 
    epoch BIGINT NOT NULL, 
    slot BIGINT NOT NULL, 
    global_slot_since_genesis BIGINT NOT NULL, 
    total_currency BIGINT, 
    num_transactions BIGINT NOT NULL, 
    num_fee_transactions BIGINT NOT NULL, 
    num_zkapp_transactions BIGINT NOT NULL
    );

CREATE TABLE transactions (
    id SERIAL PRIMARY KEY,
    height INT NOT NULL,
    block_hash TEXT NOT NULL,
    state_hash TEXT NOT NULL,
    chain_status chain_status_type NOT NULL,
    kind TEXT NOT NULL,
    source TEXT NOT NULL,
    receiver TEXT NOT NULL,
    fee_payer TEXT NOT NULL,
    token BIGINT NOT NULL,
    fee_token BIGINT NOT NULL,
    amount BIGINT NOT NULL,
    fee BIGINT NOT NULL,
    memo TEXT, 
    nonce BIGINT NOT NULL,
    failure TEXT
    );

CREATE TABLE fee_transfers (
    id SERIAL PRIMARY KEY,
    height INT NOT NULL,
    block_hash TEXT NOT NULL,
    chain_status chain_status_type NOT NULL,
    receiver TEXT NOT NULL,
    amount BIGINT NOT NULL
    );

CREATE TABLE balances (
    id SERIAL PRIMARY KEY,
    public_key TEXT NOT NULL,
    balance BIGINT NOT NULL,
    locked BIGINT,
    liquid BIGINT,
    blockheight INT NOT NULL,
    block_hash TEXT NOT NULL,
    chain_status chain_status_type NOT NULL
    )