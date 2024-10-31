FROM apache/airflow:2.7.2

USER root

RUN apt-get update && apt-get install -y openjdk-11-jdk-headless
RUN pip install -r requirements.txt

USER airflow
# RUN pip install pyspark
# RUN pip install minio