FROM ubuntu:14.04
RUN echo "#!/bin/sh\nexit 0" > /usr/sbin/policy-rc.d
RUN apt-get update
RUN apt-get install -y openssh-server git python python3 perl wget autoconf libboost-all-dev libevent-dev libminiupnpc-dev miniupnpc libtool pkg-config sudo make gcc g++ bsdmainutils 
RUN useradd --create-home -s /bin/bash namecoin
RUN mkdir -p /home/namecoin/repos
RUN git clone https://github.com/openssl/openssl /home/namecoin/repos/openssl
WORKDIR /home/namecoin/repos/openssl
RUN git checkout OpenSSL_1_0_1h
RUN ./config -fPIC shared --prefix=/usr/local/ --openssldir=/usr/local/openssl
RUN make clean
RUN make
RUN make install_sw
RUN rm -f /home/namecoin/repos/db-4.8.30.NC.tar.gz
RUN wget http://download.oracle.com/berkeley-db/db-4.8.30.NC.tar.gz -O /home/namecoin/repos/db-4.8.30.NC.tar.gz
WORKDIR /home/namecoin/repos/
RUN tar xfx db-4.8.30.NC.tar.gz
WORKDIR db-4.8.30.NC/build_unix/
RUN ../dist/configure --enable-cxx
RUN make
RUN make install
RUN rm -f /home/namecoin/repos/db-4.8.30.NC.tar.gz
RUN mkdir -p /home/namecoin/repos/namecoin-core
RUN git clone https://github.com/namecoin/namecoin-core.git /home/namecoin/repos/namecoin-core
WORKDIR /home/namecoin/repos/namecoin-core/
RUN ./autogen.sh
RUN ./configure --disable-wallet --with-pic
RUN make
RUN make install
RUN mkdir -p /home/namecoin/.namecoin/
RUN echo "rpcuser=namecoin" > /home/namecoin/.namecoin/namecoin.conf
RUN echo "rpcpassword=YOURPASSWORDHERE" >> /home/namecoin/.namecoin/namecoin.conf
RUN echo "rpcport=8336" >> /home/namecoin/.namecoin/namecoin.conf
RUN echo "rpcallowip=YOURIPHERE" >> /home/namecoin/.namecoin/namecoin.conf
RUN chown -R namecoin:namecoin /home/namecoin/
EXPOSE 8336 8334
USER namecoin
CMD [ "/usr/local/bin/namecoind", "-server", "-datadir=/data", "-conf=/home/namecoin/.namecoin/namecoin.conf" ]
