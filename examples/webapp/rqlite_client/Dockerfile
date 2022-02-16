FROM alpine:latest

ENV RQLITE_VERSION=7.3.1

RUN apk add --no-cache --update bash openssh iproute2 tcpdump net-tools screen
RUN echo "UseDNS no" >> /etc/ssh/sshd_config && \
    echo "PermitRootLogin yes" >> /etc/ssh/sshd_config && \
    echo "PasswordAuthentication yes" >> /etc/ssh/sshd_config


RUN apk update && \
    apk --no-cache add curl tar bash && \
    curl -L https://github.com/rqlite/rqlite/releases/download/v${RQLITE_VERSION}/rqlite-v${RQLITE_VERSION}-linux-amd64-musl.tar.gz -o rqlite-v${RQLITE_VERSION}-linux-amd64-musl.tar.gz && \
    tar xvfz rqlite-v${RQLITE_VERSION}-linux-amd64-musl.tar.gz && \
    cp rqlite-v${RQLITE_VERSION}-linux-amd64-musl/rqlited /bin && \
    cp rqlite-v${RQLITE_VERSION}-linux-amd64-musl/rqlite /bin && \
    rm -fr rqlite-v${RQLITE_VERSION}-linux-amd64-musl rqlite-v${RQLITE_VERSION}-linux-amd64-musl.tar.gz && \
    apk del curl tar
