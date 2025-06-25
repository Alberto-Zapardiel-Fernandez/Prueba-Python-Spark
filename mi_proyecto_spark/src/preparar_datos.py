# src/preparar_datos.py
import sqlite3
import os

def crear_bases_de_datos():
    """
    Crea y prepara las bases de datos de origen y destino para el proyecto.
    """
    # Ruta a la carpeta de datos. El '..' significa 'subir un nivel' desde src/
    ruta_data = '../data'

    # Asegurarnos de que la carpeta 'data' exista
    if not os.path.exists(ruta_data):
        os.makedirs(ruta_data)

    # --- 1. Crear y poblar la base de datos de ORIGEN ---
    print("Creando la base de datos de origen: 'fuente.db'...")
    # Creamos la ruta completa al archivo de la base de datos
    db_fuente_path = os.path.join(ruta_data, 'fuente.db')
    conn_fuente = sqlite3.connect(db_fuente_path)
    cursor_fuente = conn_fuente.cursor()

    # Crear tabla de usuarios si no existe
    cursor_fuente.execute('''
    CREATE TABLE IF NOT EXISTS usuarios (
        id INTEGER PRIMARY KEY,
        nombre TEXT NOT NULL,
        email TEXT NOT NULL UNIQUE,
        fecha_registro TEXT
    )''')

    # Insertar datos de ejemplo (usamos IGNORE para no insertar duplicados si se corre de nuevo)
    usuarios_ejemplo = [
        (1, 'Ana Torres', 'ana.t@example.com', '2023-01-15'),
        (2, 'Carlos Gomez', 'carlos.g@example.com', '2023-02-20'),
        (3, 'Luisa Fernandez', 'luisa.f@example.com', '2023-02-25'),
        (4, 'Pedro Martinez', 'pedro.m@example.com', '2023-03-10')
    ]
    cursor_fuente.executemany('INSERT OR IGNORE INTO usuarios VALUES (?,?,?,?)', usuarios_ejemplo)

    conn_fuente.commit()
    conn_fuente.close()
    print("Base de datos de origen creada con éxito.")


    # --- 2. Crear la base de datos de DESTINO (solo la estructura) ---
    print("Creando la base de datos de destino: 'destino.db'...")
    db_destino_path = os.path.join(ruta_data, 'destino.db')
    conn_destino = sqlite3.connect(db_destino_path)
    cursor_destino = conn_destino.cursor()

    # Crear la tabla donde irán los datos procesados
    # Nota: Las columnas tienen nombres diferentes y hay nuevas columnas
    cursor_destino.execute('''
    CREATE TABLE IF NOT EXISTS usuarios_activos (
        id INTEGER PRIMARY KEY,
        nombre_completo TEXT,
        correo_electronico TEXT,
        mes_registro INTEGER,
        procesado_en TEXT
    )''')

    conn_destino.commit()
    conn_destino.close()
    print("Base de datos de destino creada con éxito.")

if __name__ == "__main__":
    crear_bases_de_datos()