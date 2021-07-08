FROM alpine:latest

RUN apk add --no-cache --update bash
RUN rm /lib/libcrypto*
RUN mkdir -p /golem/input /golem/output

COPY task.sh /golem/

VOLUME /golem/input /golem/output
