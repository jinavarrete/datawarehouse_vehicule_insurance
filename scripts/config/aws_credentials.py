import os
from pathlib import Path
import boto3
import logging
from botocore.exceptions import ClientError

def get_aws_credentials():
    try:
        # Intenta obtener credenciales de variables de ambiente primero
        session = boto3.Session(
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_REGION', 'us-east-1')
        )
        logging.info("Credenciales de AWS obtenidas de variables de ambiente.")

        return session
    
    except ClientError as e:
        logging.error(f"Error al obtener credenciales de AWS: {str(e)}")
        raise 
    
