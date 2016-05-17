DROP SCHEMA namecoin CASCADE;

CREATE SCHEMA namecoin;

CREATE TABLE namecoin.blocks (
    bits TEXT,
    block_hash TEXT, 
    block_index BIGINT NOT NULL,
    block_reward NUMERIC(100,8),
    difficulty  NUMERIC(100,8), 
    extra_data TEXT,
    median_time BIGINT,
    miner_addr TEXT,
    nonce TEXT, 
    prev_block_hash TEXT, 
    size INTEGER, 
    timestamp TIMESTAMP NOT NULL,
    tx_fees NUMERIC(100,8),
    tx_volume NUMERIC(100,8),
    version INTEGER,
    PRIMARY KEY(block_hash)
);

CREATE TABLE namecoin.transactions (
    aux_block_header_hash TEXT,
    block_hash TEXT,
    fee NUMERIC(100,8),
    lock_time BIGINT,
	size INTEGER,
	tx_id TEXT NOT NULL,
	tx_index INTEGER,
    version INTEGER,
    PRIMARY KEY(tx_id),
    FOREIGN KEY(block_hash) REFERENCES namecoin.blocks(block_hash) ON DELETE CASCADE
);

CREATE TABLE namecoin.vouts (
    tx_id TEXT NOT NULL,
    value NUMERIC(100,8),
    vout_n INTEGER,
    PRIMARY KEY(tx_id, vout_n),
    FOREIGN KEY(tx_id) REFERENCES namecoin.transactions(tx_id) ON DELETE CASCADE
);


CREATE TABLE namecoin.spks (
    addresses TEXT[],
    asm TEXT,
    hex TEXT,
    req_sigs INTEGER,
    tx_id TEXT NOT NULL,
    type TEXT,
    vout_n INTEGER NOT NULL,
    PRIMARY KEY (tx_id, vout_n),
    FOREIGN KEY(tx_id, vout_n) REFERENCES namecoin.vouts(tx_id, vout_n) ON DELETE CASCADE
);

CREATE TABLE namecoin.addresses (
    address TEXT,
    block_first_seen TEXT,
    is_valid BOOLEAN,
    PRIMARY KEY(address)
);

-- It would be lovely to have something like
-- FOREIGN KEY(tx_id, vout_n) REFERENCES namecoin.vouts(tx_id, vout_n)
-- below. But we can't do that - coinbase vins do not have a tx_id or vout_n
-- that they can refer to; at the same time they are absolutely valid vins,
-- and thus need to considered when computing a TX volume, for example.
-- So we need to store them here and can't move them to a table of their own.
CREATE TABLE namecoin.vins (
    coinbase TEXT,
    script_sig JSONB,
    ref_tx_id TEXT,
    ref_vout_n INTEGER,
    sequence BIGINT,
    tx_id TEXT
);

CREATE TABLE namecoin.auxpow (
    block_hash TEXT,
    chain_index INTEGER,
    chain_merkle_branch TEXT[],
    index INTEGER,
    merkle_branch TEXT[],
    parent_block TEXT,
    tx_id TEXT,
    FOREIGN KEY(block_hash) REFERENCES namecoin.blocks(block_hash) ON DELETE CASCADE
);

CREATE TABLE namecoin.name_ops (
    block_hash TEXT NOT NULL,
    hash TEXT,
    name TEXT,
    namespace TEXT,
    op TEXT,
    rand TEXT,
    tx_id TEXT NOT NULL,
    value JSONB,
    vout_n INTEGER NOT NULL,
    PRIMARY KEY(tx_id, vout_n),
    FOREIGN KEY(block_hash) REFERENCES namecoin.blocks(block_hash) ON DELETE CASCADE,
    FOREIGN KEY(tx_id, vout_n) REFERENCES namecoin.vouts(tx_id, vout_n) ON DELETE CASCADE
);

CREATE TABLE namecoin.rare_name_ops (
    block_hash TEXT,
    json_dump JSONB,
    tx_id TEXT NOT NULL,
    vout_n INTEGER NOT NULL,
    PRIMARY KEY(tx_id, vout_n),
    FOREIGN KEY(block_hash) REFERENCES namecoin.blocks(block_hash) ON DELETE CASCADE,
    FOREIGN KEY(tx_id, vout_n) REFERENCES namecoin.vouts(tx_id, vout_n) ON DELETE CASCADE
);
