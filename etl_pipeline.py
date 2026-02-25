import requests
import pandas as pd
import boto3
import json
import logging
from sqlalchemy import create_engine
from datetime import datetime
from botocore.exceptions import ClientError

# Configuração de Logs
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configurações da API (Coordenadas de Boa Vista, RR)
API_URL = "https://api.open-meteo.com/v1/forecast?latitude=2.8197&longitude=-60.6733&current_weather=true"

# Configurações do MinIO (Simulando AWS S3)
S3_ENDPOINT = 'http://localhost:9000'
S3_ACCESS_KEY = 'admin'
S3_SECRET_KEY = 'adminpassword'
BUCKET_NAME = 'raw-weather-data'

# Configurações do PostgreSQL
DB_CONNECTION_STRING = 'postgresql://admin:adminpassword@localhost:5432/weather_dw'

def extract_data(url: str) -> dict:
    """Extrai dados da API pública com tratamento de erros."""
    try:
        logging.info("Iniciando extração de dados da API...")
        
        # Disfarce para o firewall achar que é um navegador real
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
        
        # Aumentamos o timeout e ignoramos o SSL (útil em redes com firewall restrito)
        response = requests.get(url, headers=headers, timeout=30, verify=False)
        
        response.raise_for_status() # Lança erro se o status não for 200
        return response.json()
        
    except requests.exceptions.RequestException as e:
        logging.error(f"Erro ao extrair dados da API: {e}")
        raise

def load_to_datalake(data: dict, bucket_name: str):
    """Salva o JSON bruto no Data Lake (MinIO/S3)."""
    s3_client = boto3.client(
        's3',
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY
    )
    
    # Cria o bucket se não existir
    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except ClientError:
        s3_client.create_bucket(Bucket=bucket_name)
        logging.info(f"Bucket '{bucket_name}' criado.")

    file_name = f"weather_boa_vista_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_name,
            Body=json.dumps(data)
        )
        logging.info(f"Dado bruto salvo no Data Lake: {file_name}")
    except Exception as e:
        logging.error(f"Erro ao salvar no Data Lake: {e}")
        raise

def transform_and_load_to_dw(data: dict):
    """Transforma o JSON e carrega no Data Warehouse (PostgreSQL)."""
    try:
        # Extrai apenas a parte que importa do JSON
        current_weather = data.get('current_weather', {})
        
        # Converte para DataFrame do Pandas
        df = pd.DataFrame([current_weather])
        df['extraction_date'] = datetime.now()
        
        logging.info("Dados transformados com sucesso.")
        
        # Conexão e carga no banco de dados
        engine = create_engine(DB_CONNECTION_STRING)
        df.to_sql('fact_weather', engine, if_exists='append', index=False)
        logging.info("Dados carregados no Data Warehouse (PostgreSQL) com sucesso.")
        
    except Exception as e:
        logging.error(f"Erro na transformação ou carga no DW: {e}")
        raise

def main():
    # 1. Extração
    raw_data = extract_data(API_URL)
    
    # 2. Carga no Data Lake (Raw Zone)
    load_to_datalake(raw_data, BUCKET_NAME)
    
    # 3. Transformação e Carga no DW (Trusted/Refined Zone)
    transform_and_load_to_dw(raw_data)

if __name__ == "__main__":
    main()