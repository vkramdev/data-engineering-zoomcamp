FROM python:3.9

RUN apt-get install wget
RUN pip install pandas psycopg2-binary sqlalchemy pyarrow

WORKDIR /app
COPY Ingest-NY-Taxi-Data.py Ingest-NY-Taxi-Data.py

ENTRYPOINT [ "python", "Ingest-NY-Taxi-Data.py" ]