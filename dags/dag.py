from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
import psycopg2
from sqlalchemy import create_engine

import pandas as pd
import numpy as np
from faker import Faker
import uuid
import random
import re
from datetime import datetime
from sqlalchemy.dialects.postgresql import UUID, VARCHAR, INTEGER, DATE
import requests
import json 

# Definir argumentos por defecto
default_args = {
    'owner' : 'Fernando'
}

# Función para ejecutar el script de generación de datos
def ejecutar_generate_data():
    fake = Faker()
    Faker.seed(42)
    random.seed(42)

    # Parámetros
    num_records = 5000
    error_percentage = 0.02  # 2% de errores

    # Generar datos base
    data = []
    for _ in range(num_records):
        data.append([
            str(uuid.uuid4()),  # id
            fake.name(),  # name
            fake.email(),  # email
            random.randint(18, 80),  # age
            fake.date_between(start_date='-30y', end_date='today').strftime("%Y-%m-%d")  # signup_date
        ])

    df = pd.DataFrame(data, columns=["id", "name", "email", "age", "signup_date"])

    # Introducir duplicados (alrededor del 2% del total)
    num_duplicates = int(num_records * error_percentage)
    duplicates = df.sample(num_duplicates, random_state=42)
    df = pd.concat([df, duplicates], ignore_index=True)

    # Introducir valores nulos en name, email y signup_date (2%)
    for col in ["name", "email", "signup_date"]:
        num_nulls = int(len(df) * error_percentage)
        null_indices = np.random.choice(df.index, num_nulls, replace=False)
        df.loc[null_indices, col] = None

    # Introducir errores tipográficos en name y email (2%)
    def introduce_err_tipo(text):
        if not text or len(text) < 3:
            return text
        pos = random.randint(0, len(text) - 2)
        return text[:pos] + text[pos+1] + '/' + text[pos] + text[pos+2:]  # Intercambia dos letras y añade '/'

    for col in ["name", "email"]:
        num_err = int(len(df) * error_percentage)
        indices = np.random.choice(df.index, num_err, replace=False)
        df.loc[indices, col] = df.loc[indices, col].apply(introduce_err_tipo)

    # Guardar en CSV
    file_path = "/opt/airflow/data/messy_data.csv"
    df.to_csv(file_path, index=False)

    # -------------------------------------------------------------------------------------------------------------    
    # OLLAMA_URL = "http://ollama:11434/api/generate"
    # MODEL = "llama3.1"  # Cambia por el modelo que prefieras (llama2, gemma, etc.)
    # NUM_ROWS = 5000  # Número de filas en el dataset

    # # Generar la solicitud a Ollama
    # prompt = f"Genera un conjunto de datos ficticio en formato CSV con 5000 registros. Cada fila debe contener las siguientes columnas: id (UUID), name (nombre aleatorio), email (basado en el nombre), age (edad entre 18 y 80 años) y signup_date (fecha de registro en formato YYYY-MM-DD dentro de los últimos 10 años). El resultado debe estar en texto plano como si fuera el contenido de un archivo CSV, comenzando con una línea de encabezado. No expliques tu proceso, solo genera los datos."

    # data = {
    #     "model": MODEL,
    #     "prompt": prompt,
    #     "stream": False,
    #     "options":{'num_predict': 8192}
    # }

    # headers = {
    #     "Content-Type": "application/json"
    # }
    # # Realizar la solicitud POST
    # response = requests.post(OLLAMA_URL, json=data,headers=headers)

    # # Verificar si la respuesta es válida
    # if response.status_code == 200:
    #   print(response.text)
    #   # Obtener el contenido de la respuesta (en formato texto)
    #   response_text = response.text.strip()
    
    #   # Dividir la respuesta por líneas
    #   response_lines = response_text.splitlines()
    #   # Unir todas las partes de la respuesta en una sola cadena
    #   full_response = "".join(
    #         json.loads(line)["response"] for line in response_lines
    #     )
    
    #   # Guardar el texto final en un archivo CSV
    #   with open("/opt/airflow/data/prueba_data.csv", mode="w", newline="", encoding="utf-8") as file:
    #     file.write(full_response)
    # else:
    #     print(f"Error en la generación: {response.status_code}")
    #     print(response.text)

    # -------------------------------------------------------------------------------------------------------------    

# Función para ejecutar el script de limpieza de datos
def ejecutar_clean_data():
    dataset = pd.read_csv('/opt/airflow/data/messy_data.csv')

    #Eliminamos los elementos duplicadosz
    dataset = dataset.drop_duplicates(subset = 'id')

    #Gestion valores nulos en el campo 'name', 'email' y 'signup_date'
    dataset['name'].fillna(value = 'Unknown', inplace=True) 
    dataset['email'].fillna(value = 'invalid@example.com', inplace=True)
    dataset['signup_date'].fillna(value = datetime.today().date(), inplace = True)

    #Corrección de errores tipográficos
    email_regex = r'^[a-z0-9]+[\._]?[a-z0-9]+[@]\w+[.]\w+$'                 #Expresión regular para validar 'email'
    email_valid = dataset["email"].str.match(email_regex)                   #email_valid contiene los valores booleanos. True => email valido / False => email invalido
    dataset['email'][email_valid == False] = 'invalid@email.com'            #Las filas con la condicion [email_valid == False] seran sustituidas por el valor 'invalid@email.com' en el campo 'email'

    name_regex = r"[^a-zA-ZáéíóúüñÁÉÍÓÚÜÑ .-]"                              #Expresión regular para validar 'name'
    dataset['name'].replace(name_regex, '', regex=True, inplace = True)     #Eliminamos todos los caracteres que no estan contenidos en la expresión regular

    #Conversión de fechas
    dataset['signup_date'] = pd.to_datetime(dataset['signup_date'], errors = 'coerce')      #Convertimos las fechas al formato datetime

    dataset.to_csv('/opt/airflow/data/clean_data.csv', index=False)       #Guardamos el dataset limpio en el fichero 'clean_dataset.csv'

# Función para validar los datos limpios
def ejecutar_validate_data():
    errors = []
    df = pd.read_csv('/opt/airflow/data/clean_data.csv')
    if df['name'].isnull().any():
        errors.append("Hay nombres nulos en los datos.")
    if df['email'].str.contains(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', regex=True).sum() != len(df):
        errors.append("Hay emails con formato incorrecto.")
    if df['age'].dtype != 'int':
        errors.append("Hay edades que no son números.")

    if errors:
        raise ValueError("Errores en la validación de datos: ", errors)  # Si hay errores, no continuar
    return True  # Si no hay errores, continuar

# Función para ejecutar el script de carga de datos a PostgreSQL
def ejecutar_load_to_postgres():
    # Obtener la conexión de Airflow
    hook = PostgresHook(postgres_conn_id="postgres_conn")
    engine = hook.get_sqlalchemy_engine()  # Recupera el SQLAlchemy engine
    print(engine)
    dtype_mapping = {
    'id': UUID, 
    'name': VARCHAR(50),
    'email': VARCHAR(50),
    'age': INTEGER,
    'signup_date': DATE
}

    clean_dataset = pd.read_csv('/opt/airflow/data/clean_data.csv')
    clean_dataset.to_sql('usuarios_validos', engine, index = False,if_exists='append', dtype=dtype_mapping)

# Función para ejecutar el script de carga de datos incorrectos a PostgreSQL
def ejecutar_load_invalid_data():
    # Obtener la conexión de Airflow
    hook = PostgresHook(postgres_conn_id="postgres_conn")
    engine = hook.get_sqlalchemy_engine()  # Recupera el SQLAlchemy engine

    dtype_mapping = {
    'id': UUID, 
    'name': VARCHAR(50),
    'email': VARCHAR(50),
    'age': INTEGER,
    'signup_date': DATE
}

    clean_dataset = pd.read_csv('/opt/airflow/data/messy_data.csv')
    clean_dataset.to_sql('usuarios_invalidos', engine, index = False,if_exists='append', dtype=dtype_mapping)
    

# Definir el DAG
with DAG(
    dag_id = 'data_pipeline',
    default_args = default_args,
    catchup = False,
    start_date = datetime(2025,2,1),
    schedule_interval = '@once'         #'*/15 * * * *'
) as dag:

    # Definir las tareas
    generate_task = PythonOperator(
        task_id='generate_data',
        python_callable=ejecutar_generate_data
    )

    clean_task = PythonOperator(
        task_id='clean_data',
        python_callable=ejecutar_clean_data
    )

    validate_task = PythonOperator(
    task_id='validate_data',
    python_callable=ejecutar_validate_data
    )

    # Definimos un grupo de tareas
    with TaskGroup('datos_procesados') as datos_procesados:

        truncate_table = PostgresOperator(
            task_id='truncate_table_g1',
            postgres_conn_id = 'postgres_conn',
            sql=''' DO $$ 
                    BEGIN 
                        IF EXISTS (SELECT FROM pg_tables WHERE tablename = 'usuarios_validos') THEN 
                            TRUNCATE TABLE usuarios_validos; 
                        END IF; 
                    END $$;
                '''
        )

        create_table = PostgresOperator(
            task_id='create_table_g1',
            postgres_conn_id = 'postgres_conn',
            sql = '''
                CREATE TABLE IF NOT EXISTS usuarios_validos (
                    id UUID PRIMARY KEY,
                    name VARCHAR(50),
                    email VARCHAR(50),
                    age INTEGER,
                    signup_date DATE
                )
            '''
        )
        
        load_task = PythonOperator(
            task_id='load_to_postgres_g1',
            python_callable=ejecutar_load_to_postgres
        )
        # Definir el orden de ejecución dentro del grupo de tareas
        truncate_table >> create_table >> load_task
    
    with TaskGroup('datos_sin_procesar') as datos_sin_procesar:
        
        truncate_table = PostgresOperator(
            task_id='truncate_table_g2',
            postgres_conn_id = 'postgres_conn',
            sql=''' DO $$ 
                    BEGIN 
                        IF EXISTS (SELECT FROM pg_tables WHERE tablename = 'usuarios_invalidos') THEN 
                            TRUNCATE TABLE usuarios_invalidos; 
                        END IF; 
                    END $$;
                '''
        )

        create_table = PostgresOperator(
            task_id='create_table_g2',
            postgres_conn_id = 'postgres_conn',
            sql = '''
                CREATE TABLE IF NOT EXISTS usuarios_invalidos (
                    id UUID,        
                    name VARCHAR(50),
                    email VARCHAR(50),
                    age INTEGER,
                    signup_date DATE
                )
            '''
            # No especifico id UUID como PRIMARY KEY, los datos tienen duplicados y daría un error
        )
        
        load_task = PythonOperator(
            task_id='load_to_postgres_g2',
            python_callable=ejecutar_load_invalid_data
        )
        # Definir el orden de ejecución dentro del grupo de tareas
        truncate_table >> create_table >> load_task
    
    end_pipeline = DummyOperator(
        task_id='end_pipeline',
        trigger_rule=TriggerRule.ONE_FAILED  # Esta regla asegura que se ejecute si alguna tarea falla
    )
        


    # Definir el orden de ejecución
    generate_task >> clean_task >> validate_task
    validate_task >> [datos_procesados, datos_sin_procesar]  # Si pasa la validación, continúa
    validate_task >> end_pipeline
    