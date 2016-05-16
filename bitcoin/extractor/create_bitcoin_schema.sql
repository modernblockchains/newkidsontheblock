DROP SCHEMA bitcoin CASCADE;

CREATE SCHEMA bitcoin;

CREATE TABLE bitcoin.blocks (
    bits TEXT,
    block_hash TEXT, 
    block_index BIGINT NOT NULL,
    block_reward BIGINT,
    difficulty NUMERIC(100,20),
    extra_data TEXT,
    median_time BIGINT,
    miner_addr TEXT,
    nonce TEXT, 
    prev_block_hash TEXT, 
    size INTEGER, 
    timestamp TIMESTAMP NOT NULL,
    tx_fees BIGINT,
    tx_volume BIGINT,
    version INTEGER,
    PRIMARY KEY(block_hash)
);

CREATE TABLE bitcoin.transactions (
    block_hash TEXT,
    fee BIGINT,
    lock_time BIGINT,
	size INTEGER,
	tx_id TEXT NOT NULL,
	tx_index INTEGER,
    version INTEGER,
    PRIMARY KEY(tx_id),
    FOREIGN KEY(block_hash) REFERENCES bitcoin.blocks(block_hash) ON DELETE CASCADE
);

CREATE TABLE bitcoin.vouts (
    tx_id TEXT NOT NULL,
    value BIGINT,
    vout_n INTEGER,
    PRIMARY KEY(tx_id, vout_n),
    FOREIGN KEY(tx_id) REFERENCES bitcoin.transactions(tx_id) ON DELETE CASCADE
);


CREATE TABLE bitcoin.spks (
    addresses TEXT[],
    asm TEXT,
    hex TEXT,
    req_sigs INTEGER,
    tx_id TEXT NOT NULL,
    type TEXT,
    vout_n INTEGER NOT NULL,
    PRIMARY KEY(tx_id, vout_n),
    FOREIGN KEY(tx_id, vout_n) REFERENCES bitcoin.vouts(tx_id, vout_n) ON DELETE CASCADE
);

CREATE TABLE bitcoin.addresses (
    address TEXT,
    block_first_seen TEXT,
    PRIMARY KEY(address)
);

-- It would be lovely to have something like
-- FOREIGN KEY(tx_id, vout_n) REFERENCES bitcoin.vouts(tx_id, vout_n)
-- below. But we can't do that - coinbase vins do not have a tx_id or vout_n
-- that they can refer to; at the same time they are absolutely valid vins,
-- and thus need to considered when computing a TX volume, for example.
-- So we need to store them here and can't move them to a table of their own.
-- TODO: the best way to solve this is to add the block_hash of the block where
-- the coinbase vin occurred.
CREATE TABLE bitcoin.vins (
    coinbase TEXT,
    script_sig JSONB,
    ref_tx_id TEXT,
    ref_vout_n INTEGER,
    sequence BIGINT,
    tx_id TEXT
);
