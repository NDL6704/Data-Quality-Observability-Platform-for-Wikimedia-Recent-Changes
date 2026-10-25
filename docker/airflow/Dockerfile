FROM apache/airflow:2.10.5-python3.12
RUN pip install --no-cache-dir "apache-airflow==2.10.5" \
    "psycopg[binary]==3.3.6" "pyarrow==19.0.1" "pydantic-settings==2.8.1"
RUN pip install --no-cache-dir "confluent-kafka==2.8.2"
WORKDIR /workspace
ENV PYTHONPATH=/workspace
