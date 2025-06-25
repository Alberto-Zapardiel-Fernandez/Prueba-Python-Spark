# src/etl_job.py

import os
# Importamos la nueva función date_format
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month, current_timestamp, date_format


def main():
    """
    Función principal de nuestro proceso ETL con Spark.
    """
    # --- Construcción de rutas absolutas ---
    script_dir = os.path.dirname(__file__)
    project_root = os.path.abspath(os.path.join(script_dir, '..'))
    jar_path = os.path.join(project_root, 'data', 'sqlite-jdbc-3.50.1.0.jar')  # Asegúrate que la versión es la tuya
    db_fuente_path = os.path.join(project_root, 'data', 'fuente.db')
    db_destino_path = os.path.join(project_root, 'data', 'destino.db')

    # 1. Crear la Sesión de Spark
    print("Inicializando sesión de Spark...")
    spark = SparkSession.builder \
        .appName("ETLSimpleUsuarios") \
        .config("spark.jars", jar_path) \
        .config("spark.driver.extraClassPath", jar_path) \
        .getOrCreate()

    print("Sesión de Spark creada exitosamente.")

    # --- FASE DE EXTRACCIÓN (Extract) ---
    jdbc_url_fuente = f"jdbc:sqlite:{db_fuente_path}"
    nombre_tabla_fuente = "usuarios"
    connection_properties = {
        "driver": "org.sqlite.JDBC"
    }

    print(f"Leyendo datos de la tabla '{nombre_tabla_fuente}'...")
    df_origen = spark.read.jdbc(
        url=jdbc_url_fuente,
        table=nombre_tabla_fuente,
        properties=connection_properties
    )

    df_origen.show()

    # --- FASE DE TRANSFORMACIÓN (Transform) ---
    print("Transformando los datos...")
    df_procesado = df_origen.withColumn("nombre_completo", col("nombre")) \
        .withColumn("correo_electronico", col("email")) \
        .withColumn("mes_registro", month(col("fecha_registro"))) \
        .withColumn("procesado_en", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss")) \
        .filter(col("mes_registro") > 1) \
        .select("id", "nombre_completo", "correo_electronico", "mes_registro", "procesado_en")


    print("Datos transformados. Primeras filas del resultado:")
    df_procesado.show()

    # --- FASE DE CARGA (Load) ---
    jdbc_url_destino = f"jdbc:sqlite:{db_destino_path}"
    nombre_tabla_destino = "usuarios_activos"

    print(f"Escribiendo datos en la tabla '{nombre_tabla_destino}'...")
    df_procesado.write.jdbc(
        url=jdbc_url_destino,
        table=nombre_tabla_destino,
        mode="overwrite",
        properties=connection_properties
    )

    print("Proceso ETL completado exitosamente.")

    # 7. Detener la sesión de Spark para liberar recursos
    spark.stop()

if __name__ == "__main__":
    main()