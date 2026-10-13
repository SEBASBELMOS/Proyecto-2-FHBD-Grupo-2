"""Run the statements of a .sql file in Dremio through the REST API and print the results.

Usage: docker exec g2_jupyter python /opt/airflow/scripts/run_dremio_sql.py /opt/airflow/sql/validate_dremio.sql
(DREMIO_URL defaults to http://dremio:9047; credentials admin / Admin12345.)
"""

import os
import sys
import time

import requests

URL = os.getenv("DREMIO_URL", "http://dremio:9047")
USER = os.getenv("DREMIO_USER", "admin")
PASSWORD = os.getenv("DREMIO_PASSWORD", "Admin12345")

token = requests.post(f"{URL}/apiv2/login", json={"userName": USER, "password": PASSWORD}, timeout=30).json()["token"]
headers = {"Authorization": f"_dremio{token}"}

statements = [s.strip() for s in open(sys.argv[1]).read().split(";")]
for statement in statements:
    body = "\n".join(line for line in statement.splitlines() if not line.strip().startswith("--")).strip()
    if not body:
        continue
    job_id = requests.post(f"{URL}/api/v3/sql", headers=headers, json={"sql": body}, timeout=60).json()["id"]
    while True:
        job = requests.get(f"{URL}/api/v3/job/{job_id}", headers=headers, timeout=30).json()
        if job["jobState"] in ("COMPLETED", "FAILED", "CANCELED"):
            break
        time.sleep(1)
    if job["jobState"] != "COMPLETED":
        sys.exit(f"Job {job['jobState']}: {job.get('errorMessage')}")
    rows = requests.get(f"{URL}/api/v3/job/{job_id}/results?limit=500", headers=headers, timeout=60).json()["rows"]
    print(f"-- {body.splitlines()[0][:70]} ... ({len(rows)} rows)")
    for row in rows:
        print(",".join(str(v) for v in row.values()))
