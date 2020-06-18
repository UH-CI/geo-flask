# image: ngeo/geo-flask
# Base image for building Ngeo API services in Python/flask
FROM python:3.7

COPY . /app
WORKDIR /app

RUN pip install -r requirements.txt

# set default threads for gunicorn
ENV threads=3
# set the FLASK_APP var to point to the api.py module in the default location
ENV FLASK_APP api.py

CMD python api.py
