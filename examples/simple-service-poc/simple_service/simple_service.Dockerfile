FROM python:3.8-slim
VOLUME /golem/in /golem/out
RUN pip install numpy matplotlib python-daemon
COPY simple_service.py /golem/run/simple_service.py
COPY simulate_observations.py /golem/run/simulate_observations.py
COPY simulate_observations_ctl.py /golem/run/simulate_observations_ctl.py
RUN chmod +x /golem/run/*
RUN /golem/run/simple_service.py --init
ENTRYPOINT ["sh"]
