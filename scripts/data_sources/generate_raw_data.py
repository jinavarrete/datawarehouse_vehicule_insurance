import pandas as pd
import random
import uuid
from faker import Faker
from pathlib import Path
from datetime import datetime
import logging

# Configuración del logger
def setup_logger():
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    log_filename = f"logs/data_generation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def generate_clients(n):
    fake = Faker()
    clients = []
    logger.info(f"Iniciando generación de {n} registros de clientes")
    for _ in range(n):
        client_id = str(uuid.uuid4())[:8]
        name = fake.name()
        email = fake.email() if random.random() > 0.1 else None
        phone = fake.phone_number() if random.random() > 0.1 else ""
        address = fake.address()
        clients.append([client_id, name, email, phone, address])
    return pd.DataFrame(clients, columns=["client_id", "name", "email", "phone", "address"])

def generate_crm_clients(df_clients):
    fake = Faker()
    crm_clients = []
    logger.info(f"Iniciando generación de registros de clientes de CRM")
    base_clients = df_clients.sample(frac=0.7).copy()
    for _, row in base_clients.iterrows():
        name = row['name'] if random.random() > 0.3 else row['name'].upper()
        email = row['email'] if random.random() > 0.2 else None
        phone = row['phone'] if random.random() > 0.2 else ""
        address = row['address'] if random.random() > 0.3 else ""
        iban_account_number = fake.iban() if random.random() > 0.7 else ""
        company_name = fake.company() if random.random() > 0.7 else ""
        client_type = random.choice(["gold", "silver", "bronze"])
        risk_level = random.choice(["low", "medium", "high"])
        marketing_opt_in = random.choice([True, False])
        crm_clients.append([
            row['client_id'], name, email, phone, address,
            iban_account_number, company_name,
            client_type, risk_level, marketing_opt_in
        ])
    return pd.DataFrame(
        crm_clients,
        columns=[
            "client_id", "name", "email", "phone", "address",
            "iban_account_number", "company_name",
            "client_type", "risk_level", "marketing_opt_in"
        ]
    )

def generate_vehicles(n, client_ids):
    fake = Faker()
    vehicle_brands = ["Toyota", "Honda", "Ford", "Chevrolet", "Nissan"]
    vehicle_models = ["Corolla", "Civic", "F-150", "Cruze", "Sentra"]
    years = list(range(1995, 2025))
    vehicles = []
    logger.info(f"Iniciando generación de {n} registros de vehículos")
    for _ in range(n):
        vehicle_id = str(uuid.uuid4())[:8]
        client_id = random.choice(client_ids) if random.random() > 0.05 else None
        brand = random.choice(vehicle_brands)
        model = random.choice(vehicle_models)
        year = random.choice(years)
        plate = fake.license_plate()
        vehicles.append([vehicle_id, client_id, brand, model, year, plate])
    return pd.DataFrame(vehicles, columns=["vehicle_id", "client_id", "brand", "model", "year", "plate"])

def generate_policies(n, client_ids, vehicle_ids):
    coverage_types = ["Básica", "Intermedia", "Premium"]
    statuses = ["Activa", "Vencida", "Cancelada"]
    policies = []
    logger.info(f"Iniciando generación de {n} registros de pólizas")
    for _ in range(n):
        policy_id = str(uuid.uuid4())[:8]
        client_id = random.choice(client_ids) if random.random() > 0.05 else None
        vehicle_id = random.choice(vehicle_ids) if random.random() > 0.05 else None
        coverage = random.choice(coverage_types)
        status = random.choice(statuses)
        premium = round(random.uniform(200, 3000), 2)
        policies.append([policy_id, client_id, vehicle_id, coverage, status, premium])
    return pd.DataFrame(policies, columns=["policy_id", "client_id", "vehicle_id", "coverage", "status", "premium"])

def generate_claims(n, policy_ids):
    fake = Faker()
    claim_types = ["Colisión", "Robo", "Daños por clima", "Incendio", "Otros"]
    claims = []
    logger.info(f"Iniciando generación de {n} registros de reclamaciones")
    for _ in range(n):
        claim_id = str(uuid.uuid4())[:8]
        policy_id = random.choice(policy_ids) if random.random() > 0.1 else None
        claim_date = fake.date_this_decade() if random.random() > 0.05 else "2030-01-01"
        claim_type = random.choice(claim_types)
        amount = round(random.uniform(100, 20000), 2)
        claims.append([claim_id, policy_id, claim_date, claim_type, amount])
    return pd.DataFrame(claims, columns=["claim_id", "policy_id", "claim_date", "claim_type", "amount"])

def generate_payments(n, policy_ids):
    fake = Faker()
    payments = []
    logger.info(f"Iniciando generación de {n} registros de pagos")
    for _ in range(n):
        payment_id = str(uuid.uuid4())[:8]
        policy_id = random.choice(policy_ids) if random.random() > 0.1 else None
        amount = round(random.uniform(-100, 3000), 2)
        payment_date = fake.date_this_decade()
        payments.append([payment_id, policy_id, amount, payment_date])
    return pd.DataFrame(payments, columns=["payment_id", "policy_id", "amount", "payment_date"])

if __name__ == "__main__":
    logger = setup_logger()
    logger.info("Iniciando generación de datos relacionados")

    df_clients = generate_clients(5000)
    client_ids = df_clients["client_id"].tolist()

    df_crm_clients = generate_crm_clients(df_clients)

    df_vehicles = generate_vehicles(5000, client_ids)
    vehicle_ids = df_vehicles["vehicle_id"].tolist()

    df_policies = generate_policies(5000, client_ids, vehicle_ids)
    policy_ids = df_policies["policy_id"].tolist()

    df_claims = generate_claims(2500, policy_ids)
    df_payments = generate_payments(5000, policy_ids)

    # Guardar archivos en la carpeta data_sources
    Path("data_sources").mkdir(exist_ok=True)
    df_clients.to_csv("data_sources/clients.csv", index=False)
    df_crm_clients.to_csv("data_sources/crm_clients.csv", index=False)
    df_vehicles.to_csv("data_sources/vehicles.csv", index=False)
    df_policies.to_csv("data_sources/policies.csv", index=False)
    df_claims.to_csv("data_sources/claims.csv", index=False)
    df_payments.to_csv("data_sources/payments.csv", index=False)

    logger.info("Archivos CSV generados correctamente en la carpeta data_sources.")
