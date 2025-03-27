import pandas as pd
import os 
from pathlib import Path
from datetime import datetime
import logging
import pyarrow 
from botocore.exceptions import ClientError
from scripts.config.aws_credentials import get_aws_credentials
from dotenv import load_dotenv

# Configuración del logger
def setup_logger():
    # Crear el directorio de logs si no existe
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    # Configurar el nombre del archivo de log con timestamp
    log_filename = f"logs/data_generation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    # Configurar el logger
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def read_csv_file(file_path: str, logger: logging.Logger):
    
    try:
        logger.info(f"Leyendo archivo CSV: {file_path}")
        df = pd.read_csv(file_path)
        logger.info(f"Archivo CSV leído exitosamente. Shape: {df.shape}")
        return df
    except FileNotFoundError:
        logger.error(f"Archivo no encontrado: {file_path}")
        raise
    except Exception as e:
        logger.error(f"Error al leer el archivo CSV {file_path}: {str(e)}")
        raise

def save_to_s3(df, bucket, key, logger: logging.Logger):
    
    try:
        logger.info(f"Guardando DataFrame en S3: s3://{bucket}/{key}")
        
        # Obtener sesión de AWS
        aws_session = get_aws_credentials()
        s3_client = aws_session.client('s3')
        
        # Convertir DataFrame a Parquet en memoria
        parquet_buffer = df.to_parquet()
        
        # Subir a S3
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=parquet_buffer
        )
        
        logger.info(f"DataFrame guardado exitosamente en S3: s3://{bucket}/{key}")
    
    except ClientError as e:
        logger.error(f"Error de AWS S3: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error al guardar en S3: {str(e)}")
        raise

def load_bronze_data(s3_bucket):
    
    logger.info("Iniciando carga de datos en la capa Bronze")

    # Mapeo de archivos y sus rutas en S3
    files_to_process = {
        "clients": {"source": "data_sources/clients.csv", "destination": "bronze/erp_clients.parquet"},
        "vehicles": {"source": "data_sources/vehicles.csv", "destination": "bronze/erp_vehicles.parquet"},
        "policies": {"source": "data_sources/policies.csv", "destination": "bronze/erp_policies.parquet"},
        "claims": {"source": "data_sources/claims.csv", "destination": "bronze/erp_claims.parquet"},
        "payments": {"source": "data_sources/payments.csv", "destination": "bronze/erp_payments.parquet"},
        "crm_clients": {"source": "data_sources/crm_clients.csv", "destination": "bronze/crm_clients.parquet"}
    }

    try:
        for file_name, paths in files_to_process.items():
            try:
                # Leer CSV
                df = read_csv_file(paths["source"], logger)
                
                # Guardar en S3 como Parquet
                save_to_s3(df, s3_bucket, paths["destination"], logger)
                
                logger.info(f"Procesamiento completo para {file_name}")
                
            except Exception as e:
                logger.error(f"Error procesando {file_name}: {str(e)}")
                # Continuar con el siguiente archivo en caso de error
                continue

        logger.info("Carga de datos en la capa Bronze completada")
        
    except Exception as e:
        logger.error(f"Error general en la carga de datos: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        logger = setup_logger()
        S3_BUCKET = os.getenv("S3_BUCKET")
        logger.info(f"Iniciando carga de datos en la capa Bronze en el bucket {S3_BUCKET}")
        
        # Ejecutar la carga de datos
        load_bronze_data(S3_BUCKET)
        
    except Exception as e:
        logging.error(f"Error en la ejecución principal: {str(e)}")
        raise
