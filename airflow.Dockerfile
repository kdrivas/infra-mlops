FROM apache/airflow:2.3.2

USER airflow
RUN pip install --upgrade pip setuptools wheel

COPY requirements.txt .
RUN pip install -r requirements.txt

RUN mkdir model
COPY model model/

COPY setup.py .
RUN pip install -e .