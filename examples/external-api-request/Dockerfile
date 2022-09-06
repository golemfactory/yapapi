FROM alpine:latest

RUN apk update \
 && apk add curl \
 && apk add jq \
 && rm -rf /var/cache/apk/*

COPY request.sh /golem/entrypoints/
VOLUME /golem/work /golem/output /golem/resource
