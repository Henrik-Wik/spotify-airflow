FROM apache/airflow:2.9.2
WORKDIR /usr/src/spotify_airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . . 
ENV PYTHONPATH="/usr/src/spotify_airflow/dags:/usr/src/spotify_airflow/plugins:/usr/src/spotify_airflow/config:/usr/src/spotify_airflow/tasks:/usr/src/spotify_airflow/tasks/sql:$PYTHONPATH"