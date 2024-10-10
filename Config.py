import os
from dotenv import load_dotenv
import json

# Cargar variables de entorno desde el archivo .env
load_dotenv()

# Obtener la ruta de entrada y salida desde las variables de entorno
path_DatosAbiertos = os.getenv('Path_Datos_Abiertos')
Path_Base_de_Datos = os.getenv('Path_Base_de_datos')
path = os.getenv('Directorio')

Config = os.getenv('config')

Formato = os.getenv('Formato')

url = os.getenv('url_DA')

Mensaje_html = os.getenv('Mensaje_HTML')
Mensaje_html_exito = os.getenv('Mensaje_HTML_1')
destinatarios = os.getenv('Destinatarios')

def config_odbc(path: str) -> dict:
    with open(path, 'r', encoding='utf-8') as file:
        return json.load(file)

login = config_odbc(Config)
