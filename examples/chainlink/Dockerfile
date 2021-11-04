FROM smartcontract/chainlink:1.0.0

RUN apt-get update && apt-get install -y postgresql postgresql-contrib sudo

RUN sed -i "s/local .*all .*all .*peer/local all all md5/" /etc/postgresql/*/main/pg_hba.conf

RUN mkdir /chainlink && echo 'export ROOT=/chainlink\n\
export LOG_LEVEL=debug\n\
export ETH_CHAIN_ID=42\n\
export MIN_OUTGOING_CONFIRMATIONS=2\n\
export LINK_CONTRACT_ADDRESS=0xa36085F69e2889c224210F603D836748e7dC0088\n\
export CHAINLINK_TLS_PORT=0\n\
export SECURE_COOKIES=false\n\
export GAS_UPDATER_ENABLED=true\n\
export ALLOW_ORIGINS=*\n\
export ETH_URL=wss://kovan.infura.io/ws/v3/2e16451bfd0a410d9c745ca1dfa4a007\n\
export DATABASE_URL=postgresql://chainlink:pass@/chainlink_kovan?host=/var/run/postgresql\n\
export DATABASE_TIMEOUT=0\n\
cd /chainlink\n\
echo dummy@email.invalid >api && echo dummy!!!!!PASS123 >>api && echo dummy!!!!!PASS123 >password\n\
chmod 755 /\n\
pg_ctlcluster $(ls /usr/lib/postgresql/ | sort -n | tail -n 1) main start\n\
chainlink local n --api api --password password $@ 2>/dev/null 1>/dev/null &\n\
while ! chainlink local status 2>/dev/null 1>/dev/null ; do echo "waiting for chainlink..." ; sleep 1 ; done\n\
chainlink admin login --file api\n\
chainlink keys eth list | grep ^Address: | grep -o 0x.*' >/chainlink/run.sh && chmod u+x /chainlink/run.sh

RUN /etc/init.d/postgresql start \
    && sudo -u postgres psql -c "CREATE USER chainlink WITH PASSWORD 'pass';" \
    && sudo -u postgres psql -c "CREATE DATABASE chainlink_kovan;" \
    && sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE chainlink_kovan TO chainlink;"
