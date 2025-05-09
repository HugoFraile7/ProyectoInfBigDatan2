from utils import (
    download_dataframe_from_minio,
    upload_dataframe_to_minio,
    log_data_transformation,
    validate_data_quality,
    extract_sql_to_dataframes,  
    extract_json_to_dataframe  
)
import pandas as pd
import pyarrow
import mysql


def clean_trafico(df):

    df = df.copy()

    if not pd.api.types.is_datetime64_any_dtype(df['fecha_hora']):
        df['fecha_hora'] = pd.to_datetime(df['fecha_hora'], errors='coerce')

    df = df.drop_duplicates()

    rules = {
        'no_nulls': ['fecha_hora', 'total_vehiculos'],
        'unique': []
    }
    validate_data_quality(df, 'trafico_clean', rules)

    return df

def clean_bicimad(df):

    df = df.copy()

    if not pd.api.types.is_datetime64_any_dtype(df['fecha_hora_inicio']):
        df['fecha_hora_inicio'] = pd.to_datetime(df['fecha_hora_inicio'], errors='coerce')
    if not pd.api.types.is_datetime64_any_dtype(df['fecha_hora_fin']):
        df['fecha_hora_fin'] = pd.to_datetime(df['fecha_hora_fin'], errors='coerce')

    df = df.drop_duplicates()

    rules = {
        'no_nulls': ['usuario_id', 'estacion_origen', 'estacion_destino'],
        'unique': []
    }
    validate_data_quality(df, 'bicimad_clean', rules)

    return df

def clean_parkings_merged(df):

    df = df.copy()

    if not pd.api.types.is_datetime64_any_dtype(df['fecha']):
        df['fecha'] = pd.to_datetime(df['fecha'], errors='coerce')

    # Asegurar tipos numéricos
    num_cols = ['capacidad_total', 'plazas_movilidad_reducida', 'plazas_vehiculos_electricos',
                'tarifa_hora_euros', 'latitud', 'longitud']
    for col in num_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    df = df.drop_duplicates(subset=['aparcamiento_id', 'fecha', 'hora'])

    rules = {
        'no_nulls': ['aparcamiento_id', 'fecha', 'hora', 'nombre', 'capacidad_total', 'latitud', 'longitud'],
        'unique': []
    }
    validate_data_quality(df, 'parkings_merged_clean', rules)

    return df
def clean_consumo_energetico(df):
    df = df.copy()
    df['fecha'] = pd.to_datetime(df['fecha'], errors='coerce')
    df = df.drop_duplicates(subset=['edificio_id', 'fecha'])
    rules = {
        'no_nulls': ['fecha', 'consumo_electrico_kwh', 'consumo_gas_m3'],
        'unique': []
    }
    validate_data_quality(df, 'consumo_energetico_clean', rules)
    return df

def clean_distritos(df):
    df = df.copy()
    df = df.drop_duplicates(subset=['id'])
    df['latitud'] = pd.to_numeric(df['latitud'], errors='coerce')
    df['longitud'] = pd.to_numeric(df['longitud'], errors='coerce')
    rules = {
        'no_nulls': ['nombre', 'latitud', 'longitud', 'superficie_km2'],
        'unique': []
    }
    validate_data_quality(df, 'distritos_clean', rules)
    return df

def clean_edificios_publicos(df):
    df = df.copy()
    df = df.drop_duplicates(subset=['id', 'direccion'])
    df['latitud'] = pd.to_numeric(df['latitud'], errors='coerce')
    df['longitud'] = pd.to_numeric(df['longitud'], errors='coerce')
    df['año_construccion'] = pd.to_numeric(df['año_construccion'], errors='coerce')
    df['ultimo_renovado'] = pd.to_numeric(df['ultimo_renovado'], errors='coerce')
    rules = {
        'no_nulls': ['nombre', 'tipo', 'distrito_id', 'latitud', 'longitud'],
        'unique': []
    }
    validate_data_quality(df, 'edificios_clean', rules)
    return df

def clean_estaciones_transporte(df):
    df = df.copy()
    df = df.drop_duplicates(subset=['nombre', 'linea_id'])
    df['latitud'] = pd.to_numeric(df['latitud'], errors='coerce')
    df['longitud'] = pd.to_numeric(df['longitud'], errors='coerce')
    df['año_inauguracion'] = pd.to_numeric(df['año_inauguracion'], errors='coerce')
    rules = {
        'no_nulls': ['nombre', 'linea_id', 'latitud', 'longitud'],
        'unique': []
    }
    validate_data_quality(df, 'estaciones_clean', rules)
    return df

def clean_lineas_transporte(df):
    df = df.copy()
    df = df.drop_duplicates(subset=['nombre', 'tipo'])
    df['longitud_km'] = pd.to_numeric(df['longitud_km'], errors='coerce')
    rules = {
        'no_nulls': ['nombre', 'tipo', 'longitud_km'],
        'unique': []
    }
    validate_data_quality(df, 'lineas_clean', rules)
    return df

def clean_zonas_verdes(df):
    df = df.copy()
    df['latitud'] = pd.to_numeric(df['latitud'], errors='coerce')
    df['longitud'] = pd.to_numeric(df['longitud'], errors='coerce')
    df['año_creacion'] = pd.to_numeric(df['año_creacion'], errors='coerce')
    df['tiene_area_infantil'] = df['tiene_area_infantil'].astype(bool)
    df['tiene_area_deportiva'] = df['tiene_area_deportiva'].astype(bool)
    df['tiene_area_canina'] = df['tiene_area_canina'].astype(bool)
    rules = {
        'no_nulls': ['nombre', 'distrito_id', 'latitud', 'longitud'],
        'unique': []
    }
    validate_data_quality(df, 'zonas_verdes_clean', rules)
    return df



def clean_avisamadrid(df):

    df = df.copy()

    # Convertir fechas
    df['fecha_reporte'] = pd.to_datetime(df['fecha_reporte'], errors='coerce')
    df['fecha_resolucion'] = pd.to_datetime(df['fecha_resolucion'], errors='coerce')
    df = df.drop_duplicates(subset=['id'])

    rules = {
        'no_nulls': ['id', 'categoria', 'descripcion', 'fecha_reporte', 'latitud', 'longitud'],
        'unique': ['id']
    }

    validate_data_quality(df, 'avisamadrid_clean', rules)

    return df

def main():
    print("🚀 Iniciando extracción, procesamiento y limpieza de datos...")

    # Configuración de conexión a MariaDB
    mysql_config = {
        'host': 'mariadb',
        'port': 3306,
        'user': 'municipal_user',
        'password': 'municipal_pass',
        'database': 'municipal'
    }

    sql_dfs = extract_sql_to_dataframes(
        'raw-ingestion-zone/dump-bbdd-municipal/dump-bbdd-municipal.sql',
        mysql_config=mysql_config
    )
    consumo_df = sql_dfs.get('consumo_energetico')
    distritos_df = sql_dfs.get('distritos')
    edificios_df = sql_dfs.get('edificios_publicos')
    estaciones_df = sql_dfs.get('estaciones_transporte')
    lineas_df = sql_dfs.get('lineas_transporte')
    zonas_df = sql_dfs.get('zonas_verdes')

    # Extraer JSON desde MinIO
    avisamadrid_df = extract_json_to_dataframe('raw-ingestion-zone/avisamadrid/avisamdrid.json') 

    # Extraer CSVs de MinIO como siempre
    trafico_df = download_dataframe_from_minio('raw-ingestion-zone', 'trafico/trafico-horario.csv')
    bicimad_df = download_dataframe_from_minio('raw-ingestion-zone', 'bicimad/bicimad-usos.csv')
    parkings_df = download_dataframe_from_minio('raw-ingestion-zone', 'parking/parkings-rotacion.csv')
    aparcamientos_info_df = download_dataframe_from_minio('raw-ingestion-zone', 'parking/ext_aparcamientos_info.csv')

    merged_parkings_df = parkings_df.merge(aparcamientos_info_df, on='aparcamiento_id', how='left')

    # Limpieza
    trafico_clean = clean_trafico(trafico_df)
    bicimad_clean = clean_bicimad(bicimad_df)
    parkings_clean = clean_parkings_merged(merged_parkings_df)
    consumo_clean = clean_consumo_energetico(consumo_df)
    distritos_clean = clean_distritos(distritos_df)
    edificios_clean = clean_edificios_publicos(edificios_df)
    estaciones_clean = clean_estaciones_transporte(estaciones_df)
    lineas_clean = clean_lineas_transporte(lineas_df)
    zonas_clean = clean_zonas_verdes(zonas_df)
    avisamadrid_clean = clean_avisamadrid(avisamadrid_df)

    # Subir a clean-zone
    upload_dataframe_to_minio(trafico_clean, 'clean-zone', 'trafico/trafico-horario.parquet', format='parquet')
    upload_dataframe_to_minio(bicimad_clean, 'clean-zone', 'bicimad/bicimad-usos.parquet', format='parquet')
    upload_dataframe_to_minio(parkings_clean, 'clean-zone', 'parking/merged-parkings.parquet', format='parquet')
    upload_dataframe_to_minio(consumo_clean, 'clean-zone', 'energia/consumo_energetico.parquet', format='parquet')
    upload_dataframe_to_minio(distritos_clean, 'clean-zone', 'demografia/distritos.parquet', format='parquet')
    upload_dataframe_to_minio(edificios_clean, 'clean-zone', 'urbanismo/edificios_publicos.parquet', format='parquet')
    upload_dataframe_to_minio(estaciones_clean, 'clean-zone', 'movilidad/estaciones_transporte.parquet', format='parquet')
    upload_dataframe_to_minio(lineas_clean, 'clean-zone', 'movilidad/lineas_transporte.parquet', format='parquet')
    upload_dataframe_to_minio(zonas_clean, 'clean-zone', 'medioambiente/zonas_verdes.parquet', format='parquet')
    upload_dataframe_to_minio(avisamadrid_clean, 'clean-zone', 'avisamadrid/avisamadrid.parquet', format='parquet')

    print("\nTodos los datos limpios han sido subidos a clean-zone")

if __name__ == "__main__":
    main()