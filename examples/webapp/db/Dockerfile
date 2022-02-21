FROM alpine:latest

ENV RQLITE_VERSION=7.3.1

RUN apk update && \
    apk --no-cache add curl tar bash && \
    curl -L https://github.com/rqlite/rqlite/releases/download/v${RQLITE_VERSION}/rqlite-v${RQLITE_VERSION}-linux-amd64-musl.tar.gz -o rqlite-v${RQLITE_VERSION}-linux-amd64-musl.tar.gz && \
    tar xvfz rqlite-v${RQLITE_VERSION}-linux-amd64-musl.tar.gz && \
    cp rqlite-v${RQLITE_VERSION}-linux-amd64-musl/rqlited /bin && \
    cp rqlite-v${RQLITE_VERSION}-linux-amd64-musl/rqlite /bin && \
    rm -fr rqlite-v${RQLITE_VERSION}-linux-amd64-musl rqlite-v${RQLITE_VERSION}-linux-amd64-musl.tar.gz && \
    apk del curl tar

RUN mkdir -p /rqlite/file

EXPOSE 4001 4002
COPY run_rqlite.sh /bin/run_rqlite.sh
RUN chmod a+x /bin/run_rqlite.sh
