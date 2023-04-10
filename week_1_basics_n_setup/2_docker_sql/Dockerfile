FROM python:3.9

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2
 
WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY ingest-data.py ingest-data.py

ENTRYPOINT [ "python", "ingest-data.py" ]





