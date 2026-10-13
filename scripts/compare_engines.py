"""Compare the Gold table across engines and print one labelled table.

Trino rebuilds Gold from the two Silver tables (sql/validate_trino.sql) and Dremio reads the
Gold table written by the pipeline (sql/validate_dremio.sql). Both must return the same values.

Usage: docker exec g2_jupyter python /opt/airflow/scripts/compare_engines.py
"""

import os
import sys
import time

import requests

SQL_DIR = os.getenv("SQL_DIR", "/opt/airflow/sql")
TRINO_URL = os.getenv("TRINO_URL", "http://trino:8080")
DREMIO_URL = os.getenv("DREMIO_URL", "http://dremio:9047")
DREMIO_USER = os.getenv("DREMIO_USER", "admin")
DREMIO_PASSWORD = os.getenv("DREMIO_PASSWORD", "Admin12345")

# Output is shown to a Spanish-speaking audience; ASCII only so any console renders it.
LABELS = [
    "Filas",
    "Suma de user_id",
    "Preguntas",
    "Respuestas",
    "Posts (todos los tipos)",
    "Puntaje total",
    "Suma de promedios x100",
]


def statements(file_name):
    """Return the statements of a .sql file without comment lines."""
    text = open(os.path.join(SQL_DIR, file_name)).read()
    bodies = []
    for raw in text.split(";"):
        body = "\n".join(line for line in raw.splitlines() if not line.strip().startswith("--")).strip()
        if body:
            bodies.append(body)
    return bodies


def trino(sql):
    response = requests.post(f"{TRINO_URL}/v1/statement", data=sql.encode(), headers={"X-Trino-User": "admin"}, timeout=60).json()
    rows = []
    while True:
        if "error" in response:
            sys.exit(f"Trino: {response['error']['message']}")
        rows += response.get("data", [])
        if "nextUri" not in response:
            return rows
        response = requests.get(response["nextUri"], timeout=120).json()


def dremio(sql):
    token = requests.post(f"{DREMIO_URL}/apiv2/login", json={"userName": DREMIO_USER, "password": DREMIO_PASSWORD}, timeout=30).json()["token"]
    headers = {"Authorization": f"_dremio{token}"}
    job_id = requests.post(f"{DREMIO_URL}/api/v3/sql", headers=headers, json={"sql": sql}, timeout=60).json()["id"]
    while True:
        job = requests.get(f"{DREMIO_URL}/api/v3/job/{job_id}", headers=headers, timeout=30).json()
        if job["jobState"] in ("COMPLETED", "FAILED", "CANCELED"):
            break
        time.sleep(1)
    if job["jobState"] != "COMPLETED":
        sys.exit(f"Dremio: job {job['jobState']}: {job.get('errorMessage')}")
    rows = requests.get(f"{DREMIO_URL}/api/v3/job/{job_id}/results?limit=500", headers=headers, timeout=60).json()["rows"]
    return [list(row.values()) for row in rows]


trino_sql = statements("validate_trino.sql")
replica = [int(v) for v in trino(trino_sql[0])[0]]
gold_trino = [int(v) for v in trino(trino_sql[1])[0]]
only_in_replica = int(trino(trino_sql[2])[0][0])
only_in_gold = int(trino(trino_sql[3])[0][0])
gold_dremio = [int(v) for v in dremio(statements("validate_dremio.sql")[0])[0]]
loaded = trino(
    "SELECT (SELECT CAST(max(fecha_cargue) AS VARCHAR) FROM iceberg.silver.users_hist), "
    "(SELECT CAST(max(fecha_cargue) AS VARCHAR) FROM iceberg.gold.cant_post_x_user_hist)"
)[0]

columns = ["Trino (desde Silver)", "Trino (Gold)", "Dremio (Gold)", "Iguales"]
print()
print("Comparacion de la tabla Gold (cant_post_x_user_hist) entre Trino y Dremio")
print("  Trino (desde Silver): Gold recalculada con una consulta sobre las dos tablas de Silver")
print("  Trino (Gold) y Dremio (Gold): la tabla Gold que escribio el pipeline")
print()
print(f"{'':26}" + "".join(f"{c:>22}" for c in columns[:3]) + f"{columns[3]:>10}")
all_equal = True
for label, a, b, c in zip(LABELS, replica, gold_trino, gold_dremio):
    equal = a == b == c
    all_equal = all_equal and equal
    print(f"{label:26}{a:>22,}{b:>22,}{c:>22,}{'SI' if equal else 'NO':>10}".replace(",", "."))
print()
print(f"Filas que estan en el recalculo y no en Gold: {only_in_replica}")
print(f"Filas que estan en Gold y no en el recalculo: {only_in_gold}")
print()
print(f"Ultima fecha de cargue (UTC)   Silver users_hist: {loaded[0][:19]}   Gold: {loaded[1][:19]}")
print()
identical = all_equal and only_in_replica == 0 and only_in_gold == 0
print("RESULTADO: LAS TABLAS SON IGUALES" if identical else "RESULTADO: HAY DIFERENCIAS")
sys.exit(0 if identical else 1)
