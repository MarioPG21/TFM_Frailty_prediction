#!/usr/bin/env python3
"""
Genera el fichero de contraseñas que SimpleAuthManager de Airflow 3 usa
para autenticar el login de la interfaz web.

Airflow 3 SimpleAuthManager compara contraseñas en texto plano directamente
(ver services/login.py). La ruta del fichero se lee de la variable de entorno
AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE, que apunta a config/passwords.json
dentro del volumen montado (./config) — compartido y persistente entre todos
los contenedores Airflow sin necesidad de regenerarlo en cada contenedor.
"""
import json
import os

user = os.environ.get("AIRFLOW_WWW_USER", "airflow")
pwd  = os.environ.get("AIRFLOW_WWW_PASSWORD", "airflow")
path = os.environ.get(
    "AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE",
    os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow"),
                 "simple_auth_manager_passwords.json.generated"),
)

os.makedirs(os.path.dirname(path), exist_ok=True)
with open(path, "w") as f:
    json.dump({user: pwd}, f)
os.chmod(path, 0o666)

print(f"Contraseña configurada: {path}  usuario={user}")
