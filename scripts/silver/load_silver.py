import pandas as pd
import boto3
import os
from pathlib import Path
from datetime import datetime
import logging
from io import BytesIO
from botocore.exceptions import ClientError
from scripts.config.aws_credentials import get_aws_credentials
import re

# Configurar logger
def setup_logger():
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    log_filename = f"logs/silver_cleaning_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

# Leer parquet desde S3
def read_parquet_from_s3(bucket, key, aws_session, logger):
    try:
        logger.info(f"Leyendo Parquet desde s3://{bucket}/{key}")
        s3_client = aws_session.client('s3')
        response = s3_client.get_object(Bucket=bucket, Key=key)
        df = pd.read_parquet(BytesIO(response['Body'].read()))
        logger.info(f"Datos cargados: {df.shape}")
        return df
    except Exception as e:
        logger.error(f"Error al leer desde S3: {str(e)}")
        raise

# Guardar a S3 en capa Silver
def save_parquet_to_s3(df, bucket, key, aws_session, logger):
    try:
        logger.info(f"Guardando DataFrame limpio en s3://{bucket}/{key}")
        out_buffer = BytesIO()
        df.to_parquet(out_buffer, index=False)
        s3_client = aws_session.client('s3')
        s3_client.put_object(Bucket=bucket, Key=key, Body=out_buffer.getvalue())
        logger.info(f"Archivo guardado exitosamente en s3://{bucket}/{key}")
    except Exception as e:
        logger.error(f"Error al guardar en S3: {str(e)}")
        raise

# Proceso de limpieza para clientes

def clean_clients_data(df_clients, df_crm, logger):
    logger.info("Unificando y limpiando datos de clientes")
    
    # 1. Eliminar registros sin ID en CRM
    df_crm = df_crm.dropna(subset=['client_id'])
    logger.info(f"Registros eliminados sin ID en CRM: {len(df_crm[df_crm['client_id'].isna()])}")
    
    # 2. Limpieza de campos de texto
    text_columns = ['name', 'email', 'phone', 'address', 'company_name', 'client_type', 'risk_level']
    
    # Limpieza df_clients
    for col in text_columns:
        if col in df_clients.columns:
            # Aplicar trim y convertir strings vacíos a None
            df_clients[col] = df_clients[col].apply(
                lambda x: x.strip().title() if isinstance(x, str) and x.strip() != '' else None
            )
    
    # Limpieza df_crm
    for col in text_columns:
        if col in df_crm.columns:
            # Aplicar trim y convertir strings vacíos a None
            df_crm[col] = df_crm[col].apply(
                lambda x: x.strip().title() if isinstance(x, str) and x.strip() != '' else None
            )
    
    # 3. Manejo específico para IBAN
    if 'iban_account_number' in df_crm.columns:
        df_crm['iban_account_number'] = df_crm['iban_account_number'].apply(
            lambda x: x.strip().upper() if isinstance(x, str) and x.strip() != '' else None
        )
    
    # 4. Validación de email
    def validate_email(email):
        if not isinstance(email, str) or not email:
            return None
        # Patrón básico de validación de email
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return email if re.match(pattern, email) else None
    
    df_clients['email'] = df_clients['email'].apply(validate_email)
    df_crm['email'] = df_crm['email'].apply(validate_email)
    
    # 5. Limpieza de teléfonos
    def clean_phone(phone):
        if not isinstance(phone, str) or not phone:
            return None
        # Eliminar caracteres no numéricos excepto + y -
        cleaned = re.sub(r'[^\d+\-]', '', phone)
        return cleaned if cleaned else None
    
    df_clients['phone'] = df_clients['phone'].apply(clean_phone)
    df_crm['phone'] = df_crm['phone'].apply(clean_phone)
    
    logger.info("Limpieza de datos completada")
    logger.info(f"Registros finales en df_clients: {len(df_clients)}")
    logger.info(f"Registros finales en df_crm: {len(df_crm)}")
    
    return df_crm, df_clients

def clean_vehicles_data(df_vehicles, logger):
    logger.info("Limpiando datos de vehículos")
    
    # 1. Eliminar registros sin client_id
    df_vehicles = df_vehicles.dropna(subset=['client_id'])
    logger.info(f"Registros eliminados sin client_id: {len(df_vehicles[df_vehicles['client_id'].isna()])}")
    
    # 2. Limpieza y estandarización de marcas y modelos
    df_vehicles['brand'] = df_vehicles['brand'].apply(
        lambda x: x.strip().title() if isinstance(x, str) else None
    )
    df_vehicles['model'] = df_vehicles['model'].apply(
        lambda x: x.strip().title() if isinstance(x, str) else None
    )
    
    # 3. Validación de año
    current_year = datetime.now().year
    df_vehicles['year'] = df_vehicles['year'].apply(
        lambda x: None if not isinstance(x, (int, float)) or x > current_year or x < 1900 else int(x)
    )
    
    # 4. Limpieza de patentes
    def clean_plate(plate):
        if not isinstance(plate, str) or not plate:
            return None
        # Eliminar espacios y convertir a mayúsculas
        return re.sub(r'[^A-Z0-9]', '', plate.upper())
    
    df_vehicles['plate'] = df_vehicles['plate'].apply(clean_plate)
    
    logger.info(f"Registros finales en vehicles: {len(df_vehicles)}")
    return df_vehicles

def clean_policies_data(df_policies, logger):
    logger.info("Limpiando datos de pólizas")
    
    # 1. Eliminar registros sin client_id o vehicle_id
    df_policies = df_policies.dropna(subset=['client_id', 'vehicle_id'])
    
    # 2. Estandarizar tipos de cobertura
    coverage_types = ["Básica", "Intermedia", "Premium"]
    df_policies['coverage'] = df_policies['coverage'].apply(
        lambda x: x.strip().title() if isinstance(x, str) and x.strip().title() in coverage_types else None
    )
    
    # 3. Estandarizar estados
    valid_statuses = ["Activa", "Vencida", "Cancelada"]
    df_policies['status'] = df_policies['status'].apply(
        lambda x: x.strip().title() if isinstance(x, str) and x.strip().title() in valid_statuses else None
    )
    
    # 4. Validar premium
    df_policies['premium'] = df_policies['premium'].apply(
        lambda x: round(float(x), 2) if isinstance(x, (int, float)) and x > 0 else None
    )
    
    logger.info(f"Registros finales en policies: {len(df_policies)}")
    return df_policies

def clean_claims_data(df_claims, logger):
    logger.info("Limpiando datos de reclamaciones")
    
    # 1. Eliminar registros sin policy_id
    df_claims = df_claims.dropna(subset=['policy_id'])
    
    # 2. Validar y limpiar fechas
    def clean_date(date_str):
        try:
            date = pd.to_datetime(date_str)
            return None if date > datetime.now() else date
        except:
            return None
    
    df_claims['claim_date'] = df_claims['claim_date'].apply(clean_date)
    
    # 3. Estandarizar tipos de reclamos
    valid_claim_types = ["Colisión", "Robo", "Daños Por Clima", "Incendio", "Otros"]
    df_claims['claim_type'] = df_claims['claim_type'].apply(
        lambda x: x.strip().title() if isinstance(x, str) and x.strip().title() in valid_claim_types else None
    )
    
    # 4. Validar montos
    df_claims['amount'] = df_claims['amount'].apply(
        lambda x: round(float(x), 2) if isinstance(x, (int, float)) and x > 0 else None
    )
    
    logger.info(f"Registros finales en claims: {len(df_claims)}")
    return df_claims

def clean_payments_data(df_payments, logger):
    logger.info("Limpiando datos de pagos")
    
    # 1. Eliminar registros sin policy_id
    df_payments = df_payments.dropna(subset=['policy_id'])
    
    # 2. Validar y limpiar fechas
    df_payments['payment_date'] = pd.to_datetime(df_payments['payment_date'], errors='coerce')
    df_payments = df_payments.dropna(subset=['payment_date'])
    
    # 3. Validar montos (eliminar valores negativos)
    df_payments['amount'] = df_payments['amount'].apply(
        lambda x: round(float(x), 2) if isinstance(x, (int, float)) and x > 0 else None
    )
    df_payments = df_payments.dropna(subset=['amount'])
    
    logger.info(f"Registros finales en payments: {len(df_payments)}")
    return df_payments

# Proceso principal

def process_silver_data(bucket):
    logger = setup_logger()
    aws_session = get_aws_credentials()

    try:
        # Cargar datos
        df_clients = read_parquet_from_s3(bucket, "bronze/erp_clients.parquet", aws_session, logger)
        df_crm = read_parquet_from_s3(bucket, "bronze/crm_clients.parquet", aws_session, logger)
        df_vehicles = read_parquet_from_s3(bucket, "bronze/erp_vehicles.parquet", aws_session, logger)
        df_policies = read_parquet_from_s3(bucket, "bronze/erp_policies.parquet", aws_session, logger)
        df_claims = read_parquet_from_s3(bucket, "bronze/erp_claims.parquet", aws_session, logger)
        df_payments = read_parquet_from_s3(bucket, "bronze/erp_payments.parquet", aws_session, logger)

        # Limpiar datos
        df_crm_clean, df_clients_clean = clean_clients_data(df_clients, df_crm, logger)
        df_vehicles_clean = clean_vehicles_data(df_vehicles, logger)
        df_policies_clean = clean_policies_data(df_policies, logger)
        df_claims_clean = clean_claims_data(df_claims, logger)
        df_payments_clean = clean_payments_data(df_payments, logger)

        # Guardar datos limpios
        save_parquet_to_s3(df_clients_clean, bucket, "silver/erp_clients.parquet", aws_session, logger)
        save_parquet_to_s3(df_crm_clean, bucket, "silver/crm_clients.parquet", aws_session, logger)
        save_parquet_to_s3(df_vehicles_clean, bucket, "silver/erp_vehicles.parquet", aws_session, logger)
        save_parquet_to_s3(df_policies_clean, bucket, "silver/erp_policies.parquet", aws_session, logger)
        save_parquet_to_s3(df_claims_clean, bucket, "silver/erp_claims.parquet", aws_session, logger)
        save_parquet_to_s3(df_payments_clean, bucket, "silver/erp_payments.parquet", aws_session, logger)

        logger.info("Proceso de limpieza completado exitosamente")

    except Exception as e:
        logger.error(f"Error en el proceso de limpieza: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        S3_BUCKET = os.getenv("S3_BUCKET")
        process_silver_data(S3_BUCKET)
    except Exception as e:
        logging.error(f"Error en ejecución principal: {str(e)}")
        raise
