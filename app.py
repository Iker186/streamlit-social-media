import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px
import requests 

def post_spark_job(user, repo, job, token, codeurl, dataseturl):
    url = f'https://api.github.com/repos/{user}/{repo}/dispatches'
    
    headers = {
        "Accept": "application/vnd.github.v3+json",
        "Authorization": f"token {token}"  
    }
    
    payload = {
        "event_type": job,
        "client_payload": {
            "codeurl": codeurl,
            "dataseturl": dataseturl
        }
    }
    
    response = requests.post(url, json=payload, headers=headers)
    
    if response.status_code == 204:
        st.success(f"✅ Workflow '{job}' disparado correctamente!")
    else:
        st.error(f"❌ Error al disparar el workflow '{job}': {response.status_code}")
        try:
            st.write(response.json())
        except ValueError:
            st.write(response.text)

st.header("🚀 spark-submit Job")

col1, col2 = st.columns(2)

with col1:
    github_user  = st.text_input('👤 Github user', value='Iker186')
    github_repo  = st.text_input('📦 Github repo', value='streamlit-social-media')
    spark_job    = st.text_input('🧪 Spark job', value='spark')

with col2:
    github_token = st.text_input('🔑 Github token', value='', type="password")  
    code_url     = st.text_input('🧾 Code URL', value='https://raw.githubusercontent.com/Iker186/streamlit-social-media/main/spark_process.py')
    dataset_url  = st.text_input('📊 Dataset URL', value='https://raw.githubusercontent.com/Iker186/streamlit-social-media/main/data/socialmedia.csv')

if st.button("▶️ POST spark submit"):
   post_spark_job(github_user, github_repo, spark_job, github_token, code_url, dataset_url)

# POSTGRES
def post_to_kafka_postgres():
    url = "https://producer-postgres-latest.onrender.com/send-to-kafka-postgres" 
    
    try:
        response = requests.post(url)

        if response.status_code == 200:
            st.success("✅ Datos enviados a Kafka (Postgres) correctamente!")
        else:
            st.error(f"❌ Error al enviar los datos: {response.status_code}")
            st.write(response.text)
    
    except requests.exceptions.RequestException as e:
        st.error(f"❌ Error de conexión: {e}")

if st.button("Enviar datos a postgres"):
    post_to_kafka_postgres()  

# MONGO
def post_to_kafka_mongo():
    url = "https://producer-mongo-latest.onrender.com/send-to-kafka-mongo"  
    
    try:
        response = requests.post(url)

        if response.status_code == 200:
            st.success("✅ Datos enviados a Kafka (Mongo) correctamente!")
        else:
            st.error(f"❌ Error al enviar los datos: {response.status_code}")
            st.write(response.text)
    
    except requests.exceptions.RequestException as e:
        st.error(f"❌ Error de conexión: {e}")

if st.button("Enviar datos a mongo"):
    post_to_kafka_mongo() 

# POSTGRES
def get_data_postgres():
    url = "https://consumer-postgres.onrender.com/get-data-postgres"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            df = pd.DataFrame(data)
            st.success("✅ Datos obtenidos desde Postgres:")
            st.dataframe(df)
        else:
            st.error(f"❌ Error al obtener datos: {response.status_code}")
            st.write(response.text)
    except Exception as e:
        st.error(f"❌ Error de conexión: {e}")

# MONGO
def get_data_mongo():
    url = "https://consumer-mongo.onrender.com/get-data-mongo"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            df = pd.DataFrame(data)
            st.success("✅ Datos obtenidos desde Mongo:")
            st.dataframe(df)
        else:
            st.error(f"❌ Error al obtener datos: {response.status_code}")
            st.write(response.text)
    except Exception as e:
        st.error(f"❌ Error de conexión: {e}")

col3, col4 = st.columns(2)

with col3:
    if st.button("📥 Obtener datos de Postgres"):
        get_data_postgres()

with col4:
    if st.button("📥 Obtener datos de Mongo"):
        get_data_mongo()