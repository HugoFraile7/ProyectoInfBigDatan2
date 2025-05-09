import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("postgresql+psycopg2://bbdd_postgre:bbdd_postgre@bbdd_postgre:5432/bbdd_postgre")

print("Top-10 rutas más populares (global)")
query_global_top_routes = """
SELECT
  o.estacion_id || ' → ' || d.estacion_id AS ruta,
  COUNT(*) AS total_viajes
FROM fact_viaje f
JOIN dim_estacion o ON o.estacion_sk = f.origen_sk
JOIN dim_estacion d ON d.estacion_sk = f.destino_sk
GROUP BY 1
ORDER BY total_viajes DESC
LIMIT 10;
"""
print(pd.read_sql(query_global_top_routes, engine).to_string(index=False))


print("\ntop-10 rutas por tipo de usuario (abonado vs ocasional)")
query_top_routes_by_user_type = """
WITH ranked AS (
  SELECT
    u.tipo_usuario,
    o.estacion_id || ' → ' || d.estacion_id AS ruta,
    COUNT(*) AS total_viajes,
    ROW_NUMBER() OVER (PARTITION BY u.tipo_usuario ORDER BY COUNT(*) DESC) AS rnk
  FROM fact_viaje f
  JOIN dim_usuario u ON u.usuario_sk = f.usuario_sk
  JOIN dim_estacion o ON o.estacion_sk = f.origen_sk
  JOIN dim_estacion d ON d.estacion_sk = f.destino_sk
  GROUP BY 1, 2
)
SELECT tipo_usuario, ruta, total_viajes
FROM ranked
WHERE rnk <= 10
ORDER BY tipo_usuario, total_viajes DESC;
"""
print(pd.read_sql(query_top_routes_by_user_type, engine).to_string(index=False))


print("\nEstadísticas por tipo de usuario (volumen, duración, distancia, calorías y CO2 evitado)")
query_aggregate_by_user_type = """
SELECT
  u.tipo_usuario,
  COUNT(*) AS total_viajes,
  ROUND(AVG(f.duracion_segundos)::numeric, 1) AS duracion_media_segundos,
  ROUND(AVG(f.distancia_m)::numeric, 1) AS distancia_media_metros,
  ROUND(AVG(f.calorias)::numeric, 1) AS calorias_media,
  ROUND(AVG(f.co2_evitado_g)::numeric, 1) AS co2_evitado_medio_g
FROM fact_viaje f
JOIN dim_usuario u ON u.usuario_sk = f.usuario_sk
GROUP BY u.tipo_usuario
ORDER BY total_viajes DESC;
"""
print(pd.read_sql(query_aggregate_by_user_type, engine).to_string(index=False))