docker run -d -p 48332:8332 -p 8333:8333 -v /srv/raw/virtualisation/bitcoin-docker-storage:/data --name=bitcoin_observer ralph/bitcoin:latest
