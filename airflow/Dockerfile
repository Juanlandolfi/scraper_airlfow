FROM apache/airflow:latest-python3.10

ENV PIP_USER=false

RUN python3 -m venv /opt/airflow/venvscraper

COPY requirements.txt .

RUN /opt/airflow/venvscraper/bin/pip install -r requirements.txt

ENV PIP_USER=true
