docker run -d -v /import/atp-linhome/home/1/landerson/blockchain_observatory/extractors/ethereum/to_db/:/usr/src/to_db -w /usr/src/to_db --name=ethereum_extractor python:3 save_blocks.py
