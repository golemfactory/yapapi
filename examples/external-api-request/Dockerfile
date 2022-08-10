FROM alpine:latest

RUN apk update \
 && apk add curl \
 && apk add jq \
 && rm -rf /var/cache/apk/*

WORKDIR /golem/work
