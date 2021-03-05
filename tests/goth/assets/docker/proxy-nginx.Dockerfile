FROM nginx:1.19

COPY goth/api_monitor/nginx.conf /etc/nginx/nginx.conf

COPY goth/address.py /root/address.py

SHELL ["/bin/bash", "-c"]

# This will read from /root/address.py definitions of the form:
#
#   VAR = N
#
# where VAR includes "_PORT" and N is a 4- or 5-digit number,
# and replace each {VAR} in nginx.conf with N:

RUN  grep -P '^([A-Z_]*_PORT[A-Z_]*)\s*=\s*([0-9]){4,5}$' /root/address.py \
     | while IFS=$' \t=' read VAR VALUE; do \
         sed -i "s/{$VAR}/$VALUE/g" /etc/nginx/nginx.conf;\
     done


# Try to get the address of `host.docker.internal`.
#
# If it exists, great, we can use that address to connect to host
# otherwise, we're going to use the bridge network configured in
# `docker-compose.yml` - iow, the address of the host below must point to the first
# address of the network in the compose file.
#
# the reason we need such a solution is that Docker uses a two separate mechanisms
# to allow the connections to the host - on Linux, it crates a two-way network bridge
# where the first network address on the internal network is the host.
# On the other hand, on Mac and Windows, this bridge is not available but at the same
# time, Docker provides a special address along with a respective DNS record to allow same.

RUN a=$(getent hosts host.docker.internal | awk '{ print $1 }') \
    && [[ -n "$a" ]] && HOST_ADDR=$a || HOST_ADDR="172.19.0.1" \
    && sed -i "s/{HOST_ADDR}/$HOST_ADDR/" /etc/nginx/nginx.conf
