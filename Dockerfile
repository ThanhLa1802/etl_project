FROM python:3.11-slim

WORKDIR /elt_script

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY elt_script/ app/elt_script/
COPY data/ /data/
COPY logs/ /logs/
