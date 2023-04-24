FROM python:3.9

RUN apt_get install wget
RUN pip install pandas sqlalchemy spycopg2

WORKDIR /app

COPY load-data.py load-data.py
ENTRYPOINT [ "python","load-data.py" ]