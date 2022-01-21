FROM alpine:latest

RUN apk add --no-cache --update bash openssh iproute2 tcpdump net-tools screen
RUN echo "UseDNS no" >> /etc/ssh/sshd_config && \
    echo "PermitRootLogin yes" >> /etc/ssh/sshd_config && \
    echo "PasswordAuthentication yes" >> /etc/ssh/sshd_config
