#!/usr/bin/env python3
"""
Genera el fichero de contraseñas que SimpleAuthManager de Airflow 3 usa
para autenticar el login de la interfaz web.

Airflow 3 SimpleAuthManager compara contraseñas en texto plano directamente
(ver services/login.py). El fichero debe estar en $AIRFLOW_HOME/ (no en config/).
El path se configura con AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE.
"""
import json
import os

user = os.environ.get("AIRFLOW_WWW_USER", "airflow")
pwd  = os.environ.get("AIRFLOW_WWW_PASSWORD", "airflow")
# Airflow 3 SimpleAuthManager compara en texto plano (services/login.py:
# passwords[user["username"]] == body.password). La ruta por defecto es
# $AIRFLOW_HOME/simple_auth_manager_passwords.json.generated.
path = os.path.join(
    os.environ.get("AIRFLOW_HOME", "/opt/airflow"),
    "simple_auth_manager_passwords.json.generated",
)

with open(path, "w") as f:
    json.dump({user: pwd}, f)

print(f"Contraseña configurada para usuario: {user}")
