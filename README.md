# newkidsontheblock

This is the repository for our IMC 2016 submission. It contains the source
code we used to

- extract data for the blockchains
- run the Ethereum crawler

## Data extraction

NMC, PPC, and BTC data are extracted using the official daemons, running in
Docker instances. We provide Dockerfiles.

Our extractors are intended for use with an AMQP server that couples them to a
database. We provide an extractor to write to AMQP (using pika) and then, on
the other machine, we provide the writer to the database (using pika and
psycopg2). Note that the extraction process is not meant to run in bulk mode
(unless you have a bit of time, i.e. one week in the case of BTC). We INSERT
and use a SELECT to determine the transaction volume and fees on the fly.

An AMQP server is provided as a Dockerfile for rabbitmq.

The Ethereum extractor is different: it extracts from disk (rather than using
JSON-RPC to query a daemon). It comes with a Dockerfile.

## Ethereum crawler

The crawler is essentially a modified version of geth. It should run stand-alone.

## Contact

Please contact us at modernblockchains@gmail.com for any questions you might
have in trying out the code.
