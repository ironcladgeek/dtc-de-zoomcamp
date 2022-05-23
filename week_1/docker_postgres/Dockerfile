FROM python:3.9

RUN pip install pandas sqlalchemy psycopg2 requests

WORKDIR /app
COPY data_ingestion.py data_ingestion.py

ENTRYPOINT [ "python", "data_ingestion.py" ]