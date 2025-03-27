import pandas as pd
import boto3
import os
from io import BytesIO
from datetime import datetime
from pathlib import Path
import logging
from dotenv import load_dotenv
from scripts.config.aws_credentials import get_aws_credentials

# Logger

def setup_logger():
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    log_filename = f"logs/gold_dim_vehicles_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def read_parquet_from_s3(bucket, key, session, logger):
    try:
        logger.info(f"Leyendo s3://{bucket}/{key}")
        s3 = session.client('s3')
        response = s3.get_object(Bucket=bucket, Key=key)
        return pd.read_parquet(BytesIO(response['Body'].read()))
    except Exception as e:
        logger.error(f"Error al leer {key}: {e}")
        raise

def save_parquet_to_s3(df, bucket, key, session, logger):
    try:
        logger.info(f"Guardando en s3://{bucket}/{key}")
        buffer = BytesIO()
        df.to_parquet(buffer, index=False)
        s3 = session.client('s3')
        s3.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
        logger.info("Guardado exitoso")
    except Exception as e:
        logger.error(f"Error al guardar {key}: {e}")
        raise

# Crear dimension clientes
def create_dim_clients(bucket):
    logger = setup_logger()
    load_dotenv()
    aws_session = get_aws_credentials()

    # Leer datos desde Silver
    df_clients = read_parquet_from_s3(bucket, "silver/erp_clients.parquet", aws_session, logger)
    df_crm = read_parquet_from_s3(bucket, "silver/crm_clients.parquet", aws_session, logger)

    # Seleccionar campos relevantes de CRM
    df_crm_selected = df_crm[[
        "client_id", "client_type", "risk_level", "marketing_opt_in"
    ]]

    # Enriquecer clientes
    df_dim_clients = df_clients.merge(df_crm_selected, on="client_id", how="left")

    # Validaciones finales
    logger.info(f"Clientes totales: {len(df_dim_clients)}")
    missing_crm = df_dim_clients['client_type'].isna().sum()
    logger.info(f"Clientes sin data CRM: {missing_crm}")

    # Guardar en capa GOLD
    save_parquet_to_s3(df_dim_clients, bucket, "gold/dim_clients.parquet", aws_session, logger)


# Dimensión de vehículos
def create_dim_vehicles(bucket):
    logger = setup_logger()
    load_dotenv()
    aws_session = get_aws_credentials()

    df_vehicles = read_parquet_from_s3(bucket, "silver/erp_vehicles.parquet", aws_session, logger)

    df_vehicles['vehicle_key'] = df_vehicles['vehicle_id']
    df_dim_vehicles = df_vehicles[[
        'vehicle_key', 'vehicle_id', 'client_id', 'brand', 'model', 'year', 'plate'
    ]].drop_duplicates()

    logger.info(f"Dimensión vehículos creada: {len(df_dim_vehicles)} registros")
    save_parquet_to_s3(df_dim_vehicles, bucket, "gold/dim_vehicles.parquet", aws_session, logger)
    

# Crear resumen por cliente
def create_fact_client_summary(bucket):
    logger = setup_logger()
    load_dotenv()
    aws_session = get_aws_credentials()

    df_clients = read_parquet_from_s3(bucket, "silver/erp_clients.parquet", aws_session, logger)
    df_policies = read_parquet_from_s3(bucket, "silver/erp_policies.parquet", aws_session, logger)
    df_payments = read_parquet_from_s3(bucket, "silver/erp_payments.parquet", aws_session, logger)
    df_claims = read_parquet_from_s3(bucket, "silver/erp_claims.parquet", aws_session, logger)

    # --- Polizas por cliente ---
    policies_agg = df_policies.groupby("client_id").agg(
        total_policies=('policy_id', 'count'),
        total_premium=('premium', 'sum'),
        active_policies=('status', lambda x: (x == 'Activa').sum())
    ).reset_index()

    # --- Pagos por cliente ---
    df_policies_min = df_policies[['policy_id', 'client_id']].drop_duplicates()
    df_payments = df_payments.merge(df_policies_min, on='policy_id', how='left')
    payments_agg = df_payments.groupby("client_id").agg(
        total_payments=('amount', 'sum'),
        num_payments=('payment_id', 'count'),
        last_payment_date=('payment_date', 'max')
    ).reset_index()

    # --- Reclamos por cliente ---
    df_claims = df_claims.merge(df_policies_min, on='policy_id', how='left')
    claims_agg = df_claims.groupby("client_id").agg(
        total_claims=('amount', 'sum'),
        num_claims=('claim_id', 'count')
    ).reset_index()

    # --- Join final ---
    df_summary = df_clients[['client_id']].drop_duplicates()
    df_summary = df_summary.merge(policies_agg, on='client_id', how='left')
    df_summary = df_summary.merge(payments_agg, on='client_id', how='left')
    df_summary = df_summary.merge(claims_agg, on='client_id', how='left')

    # --- Derivar métricas adicionales ---
    df_summary['payment_to_premium_ratio'] = df_summary['total_payments'] / df_summary['total_premium']
    df_summary['claim_ratio'] = df_summary['total_claims'] / df_summary['total_premium']
    df_summary['avg_payment'] = df_summary['total_payments'] / df_summary['num_payments']
    df_summary['avg_claim'] = df_summary['total_claims'] / df_summary['num_claims']

    logger.info(f"Resumen creado: {len(df_summary)} clientes")
    save_parquet_to_s3(df_summary, bucket, "gold/fact_client_summary.parquet", aws_session, logger)

if __name__ == "__main__":
    try:
        S3_BUCKET = os.getenv("S3_BUCKET")
        
        create_dim_clients(S3_BUCKET)
        
        create_dim_vehicles(S3_BUCKET)
        
        create_fact_client_summary(S3_BUCKET)
        
    except Exception as e:
        logging.error(f"Error en ejecución principal: {str(e)}")
        raise
