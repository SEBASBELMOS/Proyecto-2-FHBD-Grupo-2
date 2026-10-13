-- Runs once, when the postgres data volume is created.
-- The default database (airflow) is created by POSTGRES_DB; Nessie gets its own database
-- to hold its persistent version store (JDBC).
CREATE USER nessie WITH PASSWORD 'nessie';
CREATE DATABASE nessie OWNER nessie;
