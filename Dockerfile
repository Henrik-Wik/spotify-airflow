FROM apache/airflow:2.9.2
WORKDIR /opt/airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . . 
ENV PYTHONPATH="/opt/airflow/dags:/opt/airflow/plugins:/opt/airflow/config:/opt/airflow/tasks:/opt/airflow/tasks/sql:$PYTHONPATH"