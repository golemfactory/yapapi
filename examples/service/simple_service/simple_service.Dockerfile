FROM python:3.8-slim
VOLUME /golem/in /golem/out
COPY simple_service.py /golem/run/simple_service.py
COPY simulate_observations.py /golem/run/simulate_observations.py
COPY simulate_observations_ctl.py /golem/run/simulate_observations_ctl.py
RUN pip install numpy matplotlib
RUN python /golem/run/simple_service.py --init
ENTRYPOINT ["sh"]
