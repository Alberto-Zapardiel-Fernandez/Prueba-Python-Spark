# src/verificar_resultado.py
import sqlite3
import os


def verificar():
    """
    Se conecta a la base de datos de destino y muestra los resultados.
    """
    # --- INICIO DE LA CORRECCIÓN: Construcción de ruta absoluta ---
    # Obtener la ruta del directorio donde se encuentra este script (src/)
    script_dir = os.path.dirname(__file__)
    # Construir la ruta absoluta a la carpeta raíz del proyecto (subiendo un nivel desde src/)
    project_root = os.path.abspath(os.path.join(script_dir, '..'))
    # Construir la ruta absoluta a la base de datos de destino
    db_path = os.path.join(project_root, 'data', 'destino.db')
    # --- FIN DE LA CORRECCIÓN ---

    if not os.path.exists(db_path):
        print(f"Error: No se encontró la base de datos en {db_path}")
        return

    print(f"--- Contenido de la tabla 'usuarios_activos' en '{db_path}' ---")

    # Usamos un bloque 'with' para asegurarnos de que la conexión se cierre siempre
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()

            # Seleccionar todos los datos de la tabla
            cursor.execute("SELECT * FROM usuarios_activos")
            rows = cursor.fetchall()

            if not rows:
                print("La tabla está vacía.")
            else:
                # Imprimir cabeceras de forma dinámica
                headers = [description[0] for description in cursor.description]
                # Unimos las cabeceras para imprimirlas de forma más robusta
                print(" | ".join(f"{h:<20}" for h in headers))
                print("-" * (len(headers) * 23))

                # Imprimir filas
                for row in rows:
                    # Imprimimos cada celda con un formato estándar
                    print(" | ".join(f"{str(cell):<20}" for cell in row))
    except sqlite3.Error as e:
        print(f"Error al leer la base de datos: {e}")


if __name__ == "__main__":
    verificar()