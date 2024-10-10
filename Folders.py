import os
import logging
from pathlib import Path
import datetime
import Config
import pandas as pd

def creacion_carpeta(Formato: str, fecha_proceso: datetime.date) -> Path:
    """
    Crea la ruta de carpeta donde se almacenarán los archivos de salida.

    Args:
        Formato (str): Nombre del formato del archivo.
        fecha_proceso (datetime.date): Fecha del proceso.

    Returns:
        Path: La ruta completa de la carpeta creada.
    """
    try:
        # Crear la ruta de la carpeta
        Folder = Path(Config.path) / '01_salidas' / Formato / str(fecha_proceso.year) / \
                 (str(fecha_proceso.month).zfill(2) + '_' + Config.Mes[str(fecha_proceso.month).zfill(2)]) / \
                 str(fecha_proceso.day).zfill(2)

        # Verificar si la carpeta ya existe
        if not Folder.exists():
            logging.info(f'Creando carpeta en: {Folder}')
            Folder.mkdir(parents=True, exist_ok=True)
            logging.info(f'Carpeta creada exitosamente en: {Folder}')
        else:
            logging.info(f'La carpeta ya existe: {Folder}')

        return Folder

    except Exception as e:
        logging.error(f'Error al crear la carpeta en {Folder}: {str(e)}')
        raise
    


def eliminar_archivos_de_carpeta(ruta: str) -> None:
    """
    Elimina todos los archivos de una carpeta especificada.

    Args:
        ruta (str): Ruta de la carpeta donde se eliminarán los archivos.

    Returns:
        None
    """
    try:
        # Listar los archivos de la carpeta
        archivos = os.listdir(ruta)
        logging.info(f"Archivos encontrados en la ruta {ruta}: {archivos}")

        # Eliminar cada archivo en la lista
        for archivo in archivos:
            ruta_completa = os.path.join(ruta, archivo)

            # Verificar si es un archivo antes de eliminar
            if os.path.isfile(ruta_completa):
                logging.info(f"Eliminando archivo: {ruta_completa}")
                os.remove(ruta_completa)
            else:
                logging.warning(f"{ruta_completa} no es un archivo o ya fue eliminado.")
                
        logging.info(f"Eliminación de archivos en la ruta {ruta} completada.")
        
    except Exception as e:
        logging.error(f"Error al eliminar archivos de la carpeta {ruta}: {str(e)}")
        raise

def cargar_archivos(ruta: str, sep: str) -> pd.DataFrame:
    """
    Carga y concatena todos los archivos .txt y .csv de una carpeta especificada.

    Args:
        ruta (str): Ruta de la carpeta donde se buscarán los archivos.
        sep (str): Separador de archivos planos.

    Returns:
        pd.DataFrame: DataFrame que contiene los datos concatenados.
    """
    dfs = []
    try:
        # Listar los archivos de la carpeta
        archivos = os.listdir(ruta)
        logging.info(f"Archivos encontrados en la ruta {ruta}: {archivos}")

        # Cargar cada archivo en la lista
        for archivo in archivos:
            ruta_completa = os.path.join(ruta, archivo)

            # Verificar si es un archivo y tiene la extensión correcta
            if os.path.isfile(ruta_completa) and (archivo.endswith('.txt') or archivo.endswith('.csv')):
                logging.info(f"Archivo cargado: {ruta_completa}")
                try:
                    df = pd.read_csv(ruta_completa, sep=sep)
                    if df.empty:
                        logging.info(f"Archivo cargado No contiene Datos")
                    else:
                        dfs.append(df)
                except pd.errors.EmptyDataError:
                    # Capturar el error si el archivo está vacío
                    logging.warning(f"El archivo {ruta_completa} no contiene datos.")
            else:
                logging.warning(f"{ruta_completa} Archivo no cargado, no es .txt o .csv.")

        # Concatenar todos los DataFrames en uno solo
        if dfs:
            datos = pd.concat(dfs, ignore_index=True)
            logging.info("Carga de archivos completada.")
            return datos
        else:
            logging.warning("No se encontraron archivos válidos para cargar.")
            return pd.DataFrame()  # Retorna un DataFrame vacío si no se cargaron archivos

    except Exception as e:
        logging.error(f"Error al cargar archivos de la carpeta {ruta}: {str(e)}")
        raise