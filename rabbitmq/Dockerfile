FROM ubuntu:14.04
RUN echo "#!/bin/sh\nexit 0" > /usr/sbin/policy-rc.d
RUN useradd --create-home -s /bin/bash rabbitmq
RUN apt-key adv --keyserver keyserver.ubuntu.com --recv-keys F7B8CEA6056E8E56
RUN echo "deb http://www.rabbitmq.com/debian/ testing main" >> /etc/apt/sources.list.d/rabbitmq.list
RUN apt-get update
RUN apt-get install -y rabbitmq-server
RUN rabbitmq-plugins enable rabbitmq_management
ADD rabbitmq.config /etc/rabbitmq/rabbitmq.config
ENV RABBITMQ_LOG_BASE /data/log
ENV RABBITMQ_MNESIA_BASE /data/mnesia
VOLUME [ "/data/log", "/data/mnesia" ]
WORKDIR /data
USER rabbitmq
EXPOSE 5672 15672
CMD [ "rabbitmq-server" ]
