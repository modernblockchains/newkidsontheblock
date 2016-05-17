FROM ubuntu:14.04
RUN echo "#!/bin/sh\nexit 0" > /usr/sbin/policy-rc.d
RUN useradd --create-home -s /bin/bash ethereum 
RUN mkdir -p /home/ethereum/repos
RUN apt-get update
RUN apt-get install -y build-essential libgmp3-dev software-properties-common git
RUN apt-add-repository ppa:ethereum/ethereum
RUN apt-get update
RUN apt-get install -y golang
WORKDIR /home/ethereum/repos/
RUN git clone https://github.com/ethereum/go-ethereum
RUN chown -R ethereum:ethereum /home/ethereum
WORKDIR /home/ethereum/repos/go-ethereum
USER ethereum
RUN mkdir -p /home/ethereum/go
ENV GOPATH=/home/ethereum/go/
ENV PATH=$PATH:/home/ethereum/go/bin:/usr/local/go/bin
RUN make geth
EXPOSE 8545
EXPOSE 30303
ENTRYPOINT ["/home/ethereum/repos/go-ethereum/build/bin/geth"]
