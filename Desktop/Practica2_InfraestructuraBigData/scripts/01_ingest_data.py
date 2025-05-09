import os
from utils import upload_file_to_minio

def main():

    print("Subiendo archivos originales a MinIO...")

    files = {
        "trafico/trafico-horario.csv": "/data/raw-ingestion-zone/trafico-horario.csv",
        "parking/parkings-rotacion.csv": "/data/raw-ingestion-zone/parkings-rotacion.csv",
        "parking/ext_aparcamientos_info.csv": "/data/raw-ingestion-zone/ext_aparcamientos_info.csv",
        "bicimad/bicimad-usos.csv": "/data/raw-ingestion-zone/bicimad-usos.csv",
        "avisamadrid/avisamdrid.json":"/data/raw-ingestion-zone/avisamadrid.json",
        "dump-bbdd-municipal/dump-bbdd-municipal.sql":"/data/raw-ingestion-zone/dump-bbdd-municipal.sql"



    }

    for object_path, local_path in files.items():

        bucket = "raw-ingestion-zone"
        # Subir cada archivo a MinIO
        upload_file_to_minio(
            file_path=local_path,
            bucket_name=bucket,
            object_name=object_path
        )



    print("\nTodos los archivos fueron subidos correctamente a MinIO.")


    

if __name__ == "__main__":
    main()
