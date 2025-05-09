#!/usr/bin/env python3


import pandas as pd
from sqlalchemy import create_engine
from psycopg2.extras import execute_values
from utils import download_dataframe_from_minio


ENGINE_URL = "postgresql+psycopg2://bbdd_postgre:bbdd_postgre@bbdd_postgre:5432/bbdd_postgre"
engine = create_engine(ENGINE_URL, isolation_level="AUTOCOMMIT")
conn   = engine.raw_connection()
cur    = conn.cursor()

print("Descargando y cargando BiciMAD desde MinIO…")
df_bicimad = download_dataframe_from_minio(
    'access-zone', 'bicimad/bicimad-usos.parquet', format='parquet'
)

df_bicimad['fecha_hora_inicio'] = pd.to_datetime(df_bicimad['fecha_hora_inicio'], errors='coerce')
df_bicimad['fecha_hora_fin']    = pd.to_datetime(df_bicimad['fecha_hora_fin'],    errors='coerce')

execute_values(
    cur,
    """INSERT INTO stg_viaje (
        usuario_id, tipo_usuario, estacion_origen, estacion_destino,
        duracion_segundos, distancia_km, calorias_estimadas,
        co2_evitado_gramos, fecha_hora_inicio, fecha_hora_fin
      ) VALUES %s;""",
    df_bicimad[[
        'usuario_id', 'tipo_usuario', 'estacion_origen', 'estacion_destino',
        'duracion_segundos', 'distancia_km', 'calorias_estimadas',
        'co2_evitado_gramos', 'fecha_hora_inicio', 'fecha_hora_fin'
    ]].values.tolist()
)
print("Datos cargados en stg_viaje")

cur.execute("""
INSERT INTO dim_usuario (usuario_id, tipo_usuario)
SELECT DISTINCT usuario_id, tipo_usuario FROM stg_viaje
ON CONFLICT (usuario_id) DO NOTHING;
""")

cur.execute("""
INSERT INTO dim_estacion (estacion_id)
SELECT DISTINCT estacion_origen FROM stg_viaje
UNION
SELECT DISTINCT estacion_destino FROM stg_viaje
ON CONFLICT (estacion_id) DO NOTHING;
""")

cur.execute("""
INSERT INTO fact_viaje (
  usuario_sk, origen_sk, destino_sk,
  duracion_segundos, distancia_m, calorias, co2_evitado_g,
  fecha_hora_inicio, fecha_hora_fin
)
SELECT
  u.usuario_sk,
  o.estacion_sk,
  d.estacion_sk,
  s.duracion_segundos,
  (s.distancia_km * 1000)::int,
  s.calorias_estimadas::int,
  s.co2_evitado_gramos::int,
  s.fecha_hora_inicio,
  s.fecha_hora_fin
FROM stg_viaje s
JOIN dim_usuario  u ON u.usuario_id  = s.usuario_id
JOIN dim_estacion o ON o.estacion_id = s.estacion_origen
JOIN dim_estacion d ON d.estacion_id = s.estacion_destino;
""")
print("Datos cargados en fact_viaje")


print("Descargando y cargando parkings desde MinIO…")
df_parking = download_dataframe_from_minio(
    'access-zone', 'parking/parkings.parquet', format='parquet'
)

execute_values(
    cur,
    """INSERT INTO dim_aparcamiento (
         aparcamiento_id, nombre, direccion, capacidad_total,
         plazas_movilidad_reducida, plazas_vehiculos_electricos,
         tarifa_hora_euros, horario, latitud, longitud
       ) VALUES %s
       ON CONFLICT (aparcamiento_id) DO NOTHING;""",
    df_parking[[
        'aparcamiento_id', 'nombre', 'direccion', 'capacidad_total',
        'plazas_movilidad_reducida', 'plazas_vehiculos_electricos',
        'tarifa_hora_euros', 'horario', 'latitud', 'longitud'
    ]].drop_duplicates().values.tolist()
)
print("Datos cargados en dim_aparcamiento")


print("Descargando distritos desde MinIO (processed)…")
df_distritos = download_dataframe_from_minio(
    'processed', 'demografia/distritos.parquet', format='parquet'
)

if 'distrito_id' not in df_distritos.columns:
    df_distritos = df_distritos.reset_index(drop=True)
    df_distritos.insert(0, 'distrito_id', df_distritos.index + 1)
if 'codigo_postal' not in df_distritos.columns:
    df_distritos['codigo_postal'] = None

execute_values(
    cur,
    """INSERT INTO dim_distrito (
         distrito_id, nombre, poblacion, superficie_km2,
         densidad_poblacion, codigo_postal, latitud, longitud
       ) VALUES %s
       ON CONFLICT (distrito_id) DO NOTHING;""",
    df_distritos[[
        'distrito_id', 'nombre', 'poblacion', 'superficie_km2',
        'densidad_poblacion', 'codigo_postal', 'latitud', 'longitud'
    ]].drop_duplicates().values.tolist()
)
print("Datos cargados en dim_distrito")

# ▸ Asegurar columna 'dia_semana'
df_parking['fecha'] = pd.to_datetime(df_parking['fecha'], errors='coerce')
if 'dia_semana' not in df_parking.columns:
    df_parking['dia_semana'] = df_parking['fecha'].dt.day_name()

execute_values(
    cur,
    """INSERT INTO fact_parking_ocupacion (
         aparcamiento_id, fecha, hora, porcentaje_ocupacion, distrito_id, dia_semana
       ) VALUES %s;""",
    df_parking[[
        'aparcamiento_id', 'fecha', 'hora', 'porcentaje_ocupacion', 'distrito_id', 'dia_semana'
    ]].values.tolist()
)
print("Datos cargados en fact_parking_ocupacion")

print("⏳ Descargando estaciones de transporte desde MinIO…")
df_transporte = download_dataframe_from_minio(
    'clean-zone', 'movilidad/estaciones_transporte.parquet', format='parquet'
).drop(columns=['id'], errors='ignore')

if 'transporte_id' not in df_transporte.columns:
    df_transporte = df_transporte.reset_index(drop=True)
    df_transporte.insert(0, 'transporte_id', df_transporte.index + 1)

execute_values(
    cur,
    """INSERT INTO dim_transporte (
         transporte_id, nombre, linea_id, tipo,
         distrito_id, latitud, longitud,
         accesibilidad, correspondencia, año_inauguracion
       ) VALUES %s
       ON CONFLICT (transporte_id) DO NOTHING;""",
    df_transporte[[
        'transporte_id', 'nombre', 'linea_id', 'tipo',
        'distrito_id', 'latitud', 'longitud',
        'accesibilidad', 'correspondencia', 'año_inauguracion'
    ]].drop_duplicates().values.tolist()
)
print("Datos cargados en dim_transporte")


conn.commit()
cur.close()
conn.close()
print("Carga de datos finalizada con éxito")