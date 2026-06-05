#!/usr/bin/env python3
"""
Genera el fichero de contraseñas que SimpleAuthManager de Airflow 3 usa
para autenticar el login de la interfaz web.

Airflow 3 sustituyó el antiguo sistema FAB (Flask App Builder) por
SimpleAuthManager, que no acepta credenciales mediante variables de entorno
ni comandos CLI. En su lugar, lee un fichero JSON con el formato:
    {"usuario": "contraseña"}

Este script crea ese fichero a partir de las variables de entorno
AIRFLOW_WWW_USER y AIRFLOW_WWW_PASSWORD, definidas en .env. Se ejecuta
una sola vez durante la inicialización (airflow-init).
"""
import json
import os

user = os.environ.get("AIRFLOW_WWW_USER", "airflow")
pwd = os.environ.get("AIRFLOW_WWW_PASSWORD", "airflow")
path = "/opt/airflow/config/simple_auth_manager_passwords.json.generated"

with open(path, "w") as f:
    json.dump({user: pwd}, f)

print("Contraseña configurada para usuario: " + user)