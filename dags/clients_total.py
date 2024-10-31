from datetime import datetime, timedelta
from airflow.models import Variable
import logging
from airflow import DAG, Dataset
from airflow.decorators import task
from airflow.operators.python_operator import PythonOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from airflow.operators.bash import BashOperator
from airflow.models import Variable

import psycopg2
import json
from datetime import datetime
import os
import csv

class CargaDadosDAG:
    def __init__(self):
        self.host = Variable.get("host")
        self.database = Variable.get("database")
        self.user = Variable.get("user")
        self.password = Variable.get("password")
        self.port = Variable.get("port")
        self.jdbc_url = f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"
        self.file_path = Variable.get("json_file_path")


    def clients_total(self):
        try:
            connection = psycopg2.connect(
                host = self.host,
                database = self.database,
                user = self.user,
                password = self.password
            )
            cursor = connection.cursor()
            
            query = """
                select client_id, count(client_id) as qtd_client_id
                from patient_data
                group by client_id
            """

            cursor.execute(query)
            results = cursor.fetchall()

            print(f"Segue os resultados: {results}")

            fname = "dags/core/resultados/core/resultados/clients_total.csv"
            print(f'CAMINHO: {os.path.abspath(fname)}')

            with open(f"{os.path.abspath(fname)}", mode='w', newline='') as file:
                writer = csv.writer(file)       
                writer.writerow(["client_id", "qtd_client_id"])      
                for row in results:
                    writer.writerow(row)

            print("Os resultados foram salvos")

        except Exception as e:
            print(f"Ocorreu um erro: {e}")

        finally:
            if connection:
                cursor.close()
                connection.close()


args = {
    "owner": "airflow",
    "retries": 0,
    "start_date": datetime(2023, 1, 1),
}

dag_name = "clients_total"
carga_dados_dag = CargaDadosDAG()

with DAG(
    dag_name,
    default_args=args,
    schedule_interval=None,
) as dag:
    
    clients_total = PythonOperator(
        task_id='clients_total',
        python_callable=carga_dados_dag.clients_total,
        retries=0,
    )

clients_total 
