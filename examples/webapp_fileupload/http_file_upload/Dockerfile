FROM python:3.9-slim
RUN pip install Flask flask-sqlalchemy sqlalchemy_rqlite

RUN mkdir -p /webapp/templates
RUN mkdir -p /webapp/uploads

COPY app.py /webapp/app.py
COPY templates/index.html /webapp/templates/index.html
COPY templates/error.html /webapp/templates/error.html

VOLUME /logs
