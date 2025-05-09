#!/usr/bin/env python3


import psycopg2
from sqlalchemy import create_engine

DB_HOST = "bbdd_postgre"   # nombre del servicio en docker-compose
DB_PORT = "5432"
DB_USER = "bbdd_postgre"
DB_PASS = "bbdd_postgre"
DB_NAME = "bbdd_postgre"

# ----------------------------------------------------------------------
# 1️⃣  Comprobar / crear base de datos (fuera de transacción)
# ----------------------------------------------------------------------
print("Verificando existencia de la base de datos…")

conn = psycopg2.connect(
    dbname="postgres",
    user=DB_USER,
    password=DB_PASS,
    host=DB_HOST,
    port=DB_PORT,
)
conn.autocommit = True
try:
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (DB_NAME,))
        if cur.fetchone() is None:
            cur.execute(f"CREATE DATABASE {DB_NAME}")
            print(f"Base de datos '{DB_NAME}' creada")
        else:
            print(f"La base de datos '{DB_NAME}' ya existe")
finally:
    conn.close()

# ----------------------------------------------------------------------
# 2️⃣  Definir o actualizar el esquema del DW
# ----------------------------------------------------------------------
print("Conectando a bbdd_postgre para crear / actualizar tablas…")

engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
    isolation_level="AUTOCOMMIT",
)

DDL = """
-- Staging y dimensiones de BiciMAD
DROP TABLE IF EXISTS stg_viaje;

CREATE TABLE IF NOT EXISTS dim_usuario (
  usuario_sk   SERIAL PRIMARY KEY,
  usuario_id   INTEGER NOT NULL UNIQUE,
  tipo_usuario VARCHAR(15) NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_estacion (
  estacion_sk  SERIAL PRIMARY KEY,
  estacion_id  INTEGER NOT NULL UNIQUE
);

CREATE UNLOGGED TABLE stg_viaje (
  usuario_id            INTEGER,
  tipo_usuario          VARCHAR(15),
  estacion_origen       INTEGER,
  estacion_destino      INTEGER,
  duracion_segundos     INTEGER,
  distancia_km          DECIMAL(8, 2),
  calorias_estimadas    DECIMAL(8, 2),
  co2_evitado_gramos    DECIMAL(8, 2),
  fecha_hora_inicio     TIMESTAMP,
  fecha_hora_fin        TIMESTAMP
);

CREATE TABLE IF NOT EXISTS fact_viaje (
  viaje_sk            SERIAL PRIMARY KEY,
  usuario_sk          INTEGER NOT NULL,
  origen_sk           INTEGER NOT NULL,
  destino_sk          INTEGER NOT NULL,
  duracion_segundos   INTEGER NOT NULL,
  distancia_m         INTEGER,
  calorias            INTEGER,
  co2_evitado_g       INTEGER,
  fecha_hora_inicio   TIMESTAMP,
  fecha_hora_fin      TIMESTAMP,
  FOREIGN KEY (usuario_sk) REFERENCES dim_usuario(usuario_sk),
  FOREIGN KEY (origen_sk)  REFERENCES dim_estacion(estacion_sk),
  FOREIGN KEY (destino_sk) REFERENCES dim_estacion(estacion_sk)
);

CREATE INDEX IF NOT EXISTS idx_fact_ruta ON fact_viaje(origen_sk, destino_sk);

-- Dimensiones y hechos de aparcamientos
CREATE TABLE IF NOT EXISTS dim_aparcamiento (
  aparcamiento_id             INTEGER PRIMARY KEY,
  nombre                      TEXT,
  direccion                   TEXT,
  capacidad_total             INTEGER,
  plazas_movilidad_reducida   INTEGER,
  plazas_vehiculos_electricos INTEGER,
  tarifa_hora_euros           DECIMAL(4,2),
  horario                     TEXT,
  latitud                     DECIMAL(9,6),
  longitud                    DECIMAL(9,6)
);

CREATE TABLE IF NOT EXISTS dim_distrito (
  distrito_id        INTEGER PRIMARY KEY,
  nombre             TEXT,
  poblacion          INTEGER,
  superficie_km2     DECIMAL(6,2),
  densidad_poblacion DECIMAL(10,2),
  codigo_postal      INTEGER,
  latitud            DECIMAL(9,6),
  longitud           DECIMAL(9,6)
);

CREATE TABLE IF NOT EXISTS fact_parking_ocupacion (
  ocupacion_sk         SERIAL PRIMARY KEY,
  aparcamiento_id      INTEGER NOT NULL,
  fecha                DATE    NOT NULL,
  hora                 INTEGER NOT NULL,
  porcentaje_ocupacion DECIMAL(5,2) NOT NULL,
  dia_semana VARCHAR(15) NOT NULL,
  distrito_id          INTEGER NOT NULL,
  FOREIGN KEY (aparcamiento_id) REFERENCES dim_aparcamiento(aparcamiento_id),
  FOREIGN KEY (distrito_id)    REFERENCES dim_distrito(distrito_id)
);

CREATE INDEX IF NOT EXISTS idx_fact_parking_fecha
  ON fact_parking_ocupacion(fecha, hora);

-- Dimensión de transporte público
CREATE TABLE IF NOT EXISTS dim_transporte (
  transporte_id     INTEGER PRIMARY KEY,
  nombre            TEXT NOT NULL,
  linea_id          INTEGER NOT NULL,
  tipo              TEXT,
  distrito_id       INTEGER NOT NULL,
  latitud           DECIMAL(9,6),
  longitud          DECIMAL(9,6),
  accesibilidad     TEXT,
  correspondencia   INTEGER,
  año_inauguracion  INTEGER,
  FOREIGN KEY (distrito_id) REFERENCES dim_distrito(distrito_id)
);
"""

with engine.connect() as conn:
    conn.exec_driver_sql(DDL)
    print("Todas las tablas se han creado / actualizado correctamente en bbdd_postgre")