FROM apache/airflow:2.9.2
WORKDIR /usr/src/spotify_airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . . 