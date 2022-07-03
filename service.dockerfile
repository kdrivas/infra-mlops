FROM python:3.8-slim

RUN apt-get update && apt-get install -y libpq-dev python-dev build-essential


COPY requirements.txt .
RUN pip install -r requirements.txt

RUN mkdir model
COPY model model/

COPY setup.py .
RUN pip install -e .

WORKDIR /app
COPY app/ .

EXPOSE 8000

CMD ["uvicorn", "main:app", "--port", "8000", "--host", "0.0.0.0"]