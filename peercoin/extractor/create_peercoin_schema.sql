DROP SCHEMA peercoin CASCADE;

CREATE SCHEMA peercoin;

CREATE TABLE peercoin.blocks (
    bits TEXT,
    block_hash TEXT, 
    block_index BIGINT NOT NULL,
    block_reward BIGINT,
    difficulty NUMERIC(100,20),
    entropy_bit BOOLEAN,
    extra_data TEXT,
    flags TEXT,
    median_time BIGINT,
    miner_addr TEXT,
    mint TEXT,
    modifier TEXT,
    modifier_checksum TEXT,
    nonce TEXT, 
    prev_block_hash TEXT, 
    proof_hash TEXT,
    size INTEGER, 
    timestamp TIMESTAMP NOT NULL,
    tx_fees BIGINT,
    tx_volume BIGINT,
    version INTEGER,
    PRIMARY KEY(block_hash)
);

CREATE TABLE peercoin.transactions (
    block_hash TEXT,
    fee BIGINT,
    lock_time INTEGER,
	size INTEGER,
	tx_id TEXT NOT NULL,
	tx_index INTEGER,
    version INTEGER,
    PRIMARY KEY(tx_id),
    FOREIGN KEY(block_hash) REFERENCES peercoin.blocks(block_hash) ON DELETE CASCADE
);

CREATE TABLE peercoin.vouts (
    tx_id TEXT NOT NULL,
    value BIGINT,
    vout_n INTEGER,
    PRIMARY KEY(tx_id, vout_n),
    FOREIGN KEY(tx_id) REFERENCES peercoin.transactions(tx_id) ON DELETE CASCADE
);


CREATE TABLE peercoin.spks (
    addresses TEXT[],
    asm TEXT,
    hex TEXT,
    req_sigs INTEGER,
    tx_id TEXT NOT NULL,
    type TEXT,
    vout_n INTEGER NOT NULL,
    FOREIGN KEY(tx_id, vout_n) REFERENCES peercoin.vouts(tx_id, vout_n) ON DELETE CASCADE
);

CREATE TABLE peercoin.addresses (
    address TEXT,
    block_first_seen TEXT,
    is_valid BOOLEAN,
    PRIMARY KEY(address)
);

-- It would be lovely to have something like
-- FOREIGN KEY(tx_id, vout_n) REFERENCES peercoin.vouts(tx_id, vout_n)
-- below. But we can't do that - coinbase vins do not have a tx_id or vout_n
-- that they can refer to; at the same time they are absolutely valid vins,
-- and thus need to considered when computing a TX volume, for example.
-- So we need to store them here and can't move them to a table of their own.
CREATE TABLE peercoin.vins (
    coinbase TEXT,
    script_sig JSONB,
    ref_tx_id TEXT,
    ref_vout_n INTEGER,
    sequence BIGINT,
    tx_id TEXT
);
