FROM ubuntu:14.04
RUN echo "#!/bin/sh\nexit 0" > /usr/sbin/policy-rc.d
RUN useradd --create-home -s /bin/bash bitcoin
RUN mkdir -p /home/bitcoin/repos
RUN apt-get install -y software-properties-common
RUN add-apt-repository ppa:bitcoin/bitcoin
RUN apt-get update
RUN apt-get install -y build-essential libtool autotools-dev automake pkg-config libssl-dev libevent-dev bsdmainutils libboost-all-dev libminiupnpc-dev libdb4.8-dev libdb4.8++-dev git
RUN git clone https://github.com/bitcoin/bitcoin.git /home/bitcoin/repos/bitcoin
WORKDIR /home/bitcoin/repos/bitcoin
RUN ./autogen.sh
RUN ./configure
RUN make
RUN make install
RUN chown -R bitcoin:bitcoin /home/bitcoin/
RUN mkdir -p /home/bitcoin/.bitcoin/
RUN echo "rpcuser=bitcoin" > /home/bitcoin/.bitcoin/bitcoin.conf
RUN echo "rpcpassword=YOURPASSWORDHERE" >> /home/bitcoin/.bitcoin/bitcoin.conf
RUN echo "rpcport=8332" >> /home/bitcoin/.bitcoin/bitcoin.conf
RUN echo "rpcallowip=YOURIPHERE" >> /home/bitcoin/.bitcoin/bitcoin.conf
RUN chown -R bitcoin:bitcoin /home/bitcoin/
EXPOSE 8332 8333
USER bitcoin
CMD [ "/usr/local/bin/bitcoind", "-server", "-datadir=/data", "-conf=/home/bitcoin/.bitcoin/bitcoin.conf" ]
