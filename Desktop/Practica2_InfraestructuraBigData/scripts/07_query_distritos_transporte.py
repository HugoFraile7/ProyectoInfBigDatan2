import pandas as pd
from sqlalchemy import create_engine
from scipy.stats import pearsonr   # para la correlación


engine = create_engine(
    "postgresql+psycopg2://bbdd_postgre:bbdd_postgre@bbdd_postgre:5432/bbdd_postgre"
)

query_infra_por_distrito = """
SELECT
  d.distrito_id,
  d.nombre,
  d.densidad_poblacion,
  d.superficie_km2,
  COUNT(t.distrito_id)                      AS num_infra,
  ROUND(COUNT(t.distrito_id)::numeric / NULLIF(d.superficie_km2,0), 3)
                                            AS infra_por_km2
FROM dim_distrito      d
LEFT JOIN dim_transporte t
       ON t.distrito_id = d.distrito_id
GROUP BY d.distrito_id, d.nombre, d.densidad_poblacion, d.superficie_km2
ORDER BY d.distrito_id;
"""
df = pd.read_sql(query_infra_por_distrito, engine)

print("Infraestructura de transporte por distrito")
print(df.to_string(index=False))

corr_abs, p_abs   = pearsonr(df['densidad_poblacion'], df['num_infra'])
corr_km2, p_km2   = pearsonr(df['densidad_poblacion'], df['infra_por_km2'])

print("\nCorrelación entre densidad de población y nº de infraestructuras")
print(f"  • Correlación densidad ↔︎ número absoluto de infra:   r = {corr_abs:.3f}  (p = {p_abs:.4f})")
print(f"  • Correlación densidad ↔︎ infra/ km²:                 r = {corr_km2:.3f}  (p = {p_km2:.4f})")


TOP_N = 10
print(f"\nTop-{TOP_N} distritos por infraestructuras absolutas")
print(
    df.sort_values('num_infra', ascending=False)
      .head(TOP_N)[['nombre', 'num_infra', 'densidad_poblacion']]
      .to_string(index=False)
)

print(f"\nTop-{TOP_N} distritos por infraestructuras por km²")
print(
    df.sort_values('infra_por_km2', ascending=False)
      .head(TOP_N)[['nombre', 'infra_por_km2', 'densidad_poblacion']]
      .to_string(index=False)
)


print("\nEstadísticos descriptivos")
print(df[['densidad_poblacion', 'num_infra', 'infra_por_km2']].describe().round(2))