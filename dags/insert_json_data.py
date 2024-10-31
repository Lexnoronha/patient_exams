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

class CargaDadosDAG:
    def __init__(self):
        self.host = Variable.get("host")
        self.database = Variable.get("database")
        self.user = Variable.get("user")
        self.password = Variable.get("password")
        self.port = Variable.get("port")
        self.jdbc_url = f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"
        self.file_path = Variable.get("json_file_path")


    def insert_json_data(self):

        def parse_date(date_str):
            try:
                return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            except ValueError:
                return None

        fname = "file1.txt"
        print(f'CAMINHO: {os.path.abspath(fname)}')

        with open("/opt/airflow/dags/core/raw/caduceus.consolidation_anonymized.json", "r", encoding="utf-8", errors="ignore") as file:
        #with open(self.file_path, "r", encoding="utf-8", errors="ignore") as file:
            data = json.load(file)

        conn = psycopg2.connect(
                host = self.host,
                database = self.database,
                user = self.user,
                password = self.password
        )
        cursor = conn.cursor()


        print('Criando Tabela se não existir...')
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS patient_data (
            id TEXT PRIMARY KEY,
            patient_id TEXT,
            client_id TEXT,
            last_utilization_date TIMESTAMPTZ,
            sex TEXT,
            update_date TIMESTAMPTZ,
            chronic_kidney_disease JSONB,
            diabetes_mellitus JSONB,
            dyslipidemia JSONB,
            hypertension JSONB,
            obesity JSONB
        )
        ''')

        print('Limpando Tabela...')
        cursor.execute('''
            TRUNCATE TABLE patient_data 
        ''')

        def convert_to_json(data):
            return json.dumps(data) if data else None

        print('Inserindo dados na Tabela...')
        for entry in data:
            cursor.execute('''
                INSERT INTO patient_data (
                    id, patient_id, client_id, last_utilization_date, sex, update_date,
                    chronic_kidney_disease, diabetes_mellitus, dyslipidemia, hypertension, obesity
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''', (
                entry["_id"]["$oid"],
                entry["patient_id"],
                entry["client_id"],
                parse_date(entry["last_utilization_date"]["$date"]),
                entry["sex"],
                parse_date(entry["update_date"]["$date"]),
                convert_to_json(entry["ChronicKidneyDisease"]),
                convert_to_json(entry["DiabetesMellitus"]),
                convert_to_json(entry["Dyslipidemia"]),
                convert_to_json(entry["HAS"]),
                convert_to_json(entry["Obesity"])
            ))

        conn.commit()
        cursor.close()
        conn.close()

args = {
    "owner": "airflow",
    "retries": 0,
    "start_date": datetime(2023, 1, 1),
}

dag_name = "insert_json_data"
carga_dados_dag = CargaDadosDAG()

with DAG(
    dag_name,
    default_args=args,
    schedule_interval=None,
) as dag:
    
    insert_json_data = PythonOperator(
        task_id='insert_json_data',
        python_callable=carga_dados_dag.insert_json_data,
        retries=0,
    )

insert_json_data 
