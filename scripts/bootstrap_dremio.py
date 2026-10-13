"""Bootstrap Dremio through its REST API (idempotent).

1. Creates the first (admin) user if Dremio is brand new.
2. Registers the Nessie catalog as a source named `nessie` (data in MinIO, path-style S3).

Credentials: user `admin` / password `Admin12345` (override with DREMIO_USER / DREMIO_PASSWORD).
"""

import os
import sys
import time

import requests

DREMIO_URL = os.getenv("DREMIO_URL", "http://dremio:9047")
USER = os.getenv("DREMIO_USER", "admin")
PASSWORD = os.getenv("DREMIO_PASSWORD", "Admin12345")
SOURCE_NAME = "nessie"

NESSIE_SOURCE = {
    "entityType": "source",
    "name": SOURCE_NAME,
    "type": "NESSIE",
    "config": {
        "nessieEndpoint": os.getenv("NESSIE_V2_URI", "http://nessie:19120/api/v2"),
        "nessieAuthType": "NONE",
        "asyncEnabled": True,
        "credentialType": "ACCESS_KEY",
        "awsAccessKey": os.getenv("AWS_ACCESS_KEY_ID", "admin"),
        "awsAccessSecret": os.getenv("AWS_SECRET_ACCESS_KEY", "password"),
        "awsRootPath": "/gold",
        "secure": False,
        "propertyList": [
            {"name": "fs.s3a.path.style.access", "value": "true"},
            {"name": "fs.s3a.endpoint", "value": "minio:9000"},
            {"name": "dremio.s3.compat", "value": "true"},
        ],
    },
}


def wait_for_dremio(timeout=600):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if "OK" in requests.get(f"{DREMIO_URL}/apiv2/server_status", timeout=5).text:
                return
        except requests.RequestException:
            pass
        time.sleep(5)
    sys.exit("Dremio did not become ready in time")


def login():
    response = requests.post(f"{DREMIO_URL}/apiv2/login", json={"userName": USER, "password": PASSWORD}, timeout=30)
    return response.json()["token"] if response.status_code == 200 else None


def main():
    wait_for_dremio()
    token = login()
    if token is None:
        response = requests.put(
            f"{DREMIO_URL}/apiv2/bootstrap/firstuser",
            headers={"Authorization": "_dremionull"},
            json={
                "userName": USER,
                "firstName": "Admin",
                "lastName": "Grupo2",
                "email": "admin@example.com",
                "createdAt": int(time.time() * 1000),
                "password": PASSWORD,
            },
            timeout=60,
        )
        response.raise_for_status()
        print(f"First user '{USER}' created")
        token = login()
    if token is None:
        sys.exit("Could not log in to Dremio")

    headers = {"Authorization": f"_dremio{token}"}
    existing = requests.get(f"{DREMIO_URL}/api/v3/catalog/by-path/{SOURCE_NAME}", headers=headers, timeout=30)
    if existing.status_code == 200:
        print(f"Source '{SOURCE_NAME}' already registered")
        return
    created = requests.post(f"{DREMIO_URL}/api/v3/catalog", headers=headers, json=NESSIE_SOURCE, timeout=120)
    if created.status_code >= 300:
        sys.exit(f"Could not create source: HTTP {created.status_code} {created.text}")
    print(f"Source '{SOURCE_NAME}' created")


if __name__ == "__main__":
    main()
