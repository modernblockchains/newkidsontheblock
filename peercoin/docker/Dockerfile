FROM ubuntu:14.04
RUN echo "#!/bin/sh\nexit 0" > /usr/sbin/policy-rc.d
RUN useradd --create-home -s /bin/bash peercoin 
RUN mkdir -p /home/peercoin/repos
RUN apt-get update
RUN apt-get install -y build-essential libtool autotools-dev automake pkg-config libssl-dev libevent-dev bsdmainutils libboost-all-dev libminiupnpc-dev software-properties-common g++ git
RUN add-apt-repository ppa:bitcoin/bitcoin
RUN apt-get update
RUN apt-get install -y libdb4.8-dev libdb4.8++-dev
RUN git clone https://github.com/ppcoin/ppcoin.git /home/peercoin/repos/peercoin
WORKDIR /home/peercoin/repos/peercoin/src
RUN make -f makefile.unix
RUN mkdir -p /home/peercoin/.peercoin/
RUN echo "rpcuser=peercoin" > /home/peercoin/.peercoin/peercoin.conf
RUN echo "rpcpassword=YOURPASSWORDHERE" >> /home/peercoin/.peercoin/peercoin.conf
RUN echo "rpcallowip=YOURIPHERE" >> /home/peercoin/.peercoin/peercoin.conf
RUN chown -R peercoin:peercoin /home/peercoin/
EXPOSE 9901 9902 
USER peercoin
CMD [ "/home/peercoin/repos/peercoin/src/ppcoind", "-server", "-datadir=/data", "-conf=/home/peercoin/.peercoin/peercoin.conf" ]
