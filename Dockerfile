FROM python:3.10
WORKDIR /usr/src/spotify_airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . . 