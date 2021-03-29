FROM python:3.8-slim
VOLUME /golem/html /golem/out /golem/test
COPY run-http-server.sh /golem/run
EXPOSE 80
ENTRYPOINT ["sh", "/golem/run/run-http-server.sh"]
