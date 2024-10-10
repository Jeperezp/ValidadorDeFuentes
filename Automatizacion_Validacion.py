# %%
import Config
import Outlook
import Types
import Folders
import pandas as pd
import datetime
import os,logging, holidays_co
import Utils
import requests
import numpy as np
import sys

# %%
today = datetime.date.today()

log_directory = os.path.join(os.getcwd(), "logs")

# Si no existe el directorio, lo crea
if not os.path.exists(log_directory):
    os.makedirs(log_directory)

# Configuración del archivo de log
log_file = os.path.join(log_directory, today.strftime('%Y-%m-%d')+"Archivo_control.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()  # También imprime en consola
    ]
)

# %%
Folder = Folders.creacion_carpeta(
    Formato='F-523',
    fecha_proceso=today
)

# Crea un DataFrame con los Feriados del Año
df_holidays = pd.DataFrame(data=holidays_co.get_colombia_holidays_by_year(today.year),
                           columns=["Fecha","Celebracion"])

df_holidays["Fecha"] = pd.to_datetime(df_holidays["Fecha"])

# Verificar si hoy es fin de semana
if Utils.verificar_fecha(today,df_holidays):
    pass
else:
    sys.exit()

print(Utils.verificar_fecha(today,df_holidays))

paths = [Config.Path_Base_de_Datos,Config.path_DatosAbiertos]
for path in paths:
    Folders.eliminar_archivos_de_carpeta(path)

# %%
Fecha_inicio = datetime.date(year=2016,month=1,day=1) #Periodo Inicio que esta cargado en Datos Abiertos
Fecha_Fin  = Utils.date_proc(today,2) # Periodo de T-2 
periodos = Utils.pares(Fecha_inicio,Fecha_Fin) # Lista de Pares ordenados desde el 2016 hasta la fecha de t-2

# %%
logging.info(f'El periodo de datos evaluados son: {Fecha_inicio} - {Fecha_Fin}')

with open(Config.Formato, 'r', encoding='utf-8') as file:
    query_template = file.read()

# Ciclo que ejecuta la consulta SQL por cada pareja ordenada de la lista periodos
for i in range(len(periodos)):
    query = query_template.replace('Fecha_I', periodos[i][0])
    query = query.replace('Fecha_F', periodos[i][1])
    
    try:
        Datos = Utils.conexion_bd(query, "Base_de_Datos")
        Datos.to_csv(Config.Path_Base_de_Datos + "//" + periodos[i][1] + ".txt", index=False, sep='|')
        logging.info(f'Datos Procesados Base_de_Datos periodo {periodos[i][0]} al {periodos[i][1]} ')
    except Exception as e:
        logging.error(f"Error al procesar el periodo {periodos[i]}: {str(e)}")


# %%
try:
    response = requests.get(Config.url_DA)
    response.raise_for_status() 
    logging.info(f'La conexion a la URL {Config.url_DA} es Exitosa')
    pass
except requests.exceptions.SSLError as ssl_err:
    logging.error(f"Error SSL: {ssl_err}")
    Outlook.manejar_error(today)

except requests.exceptions.RequestException as req_err:
    logging.error(f"Ocurrió un error en la solicitud: {req_err}")
    Outlook.manejar_error(today)


# %%
# ciclo que realiza la peticion a la URL por cada pareja ordenada de la lista periodos y exporta el archivo a la ruta de destino
for i in range(len(periodos)):
    Datos = Utils.Connection_DatosAbiertos(periodos[i][0],periodos[i][1],Config.url_DA,'fecha_corte')
    Datos.to_csv(Config.path_DatosAbiertos+"//"+periodos[i][1]+".txt", index=False, sep='|')
    logging.info(f'Datos Procesados Datos Abiertos periodo {periodos[i][0]} al {periodos[i][1]} ')

# %%
df_Base_de_Datos = Folders.cargar_archivos(paths[0], '|')
df_Datos_Abiertos = Folders.cargar_archivos(paths[1], '|')
logging.info(f'Total registros cargados de Base_de_Datos: {df_Base_de_Datos.shape[0]}')
logging.info(f'Total registros cargados de Datos Abiertos:{df_Datos_Abiertos.shape[0]}')

base_date = datetime.datetime(1900, 1, 1)
def fecha_a_numero_serie(fecha):
    diferencia = (fecha - base_date).days
    return diferencia+2

df_Datos_Abiertos = df_Datos_Abiertos[['fecha_corte','tipo_entidad','codigo_entidad','tipo_negocio','subtipo_negocio','codigo_negocio','tipo_participacion']]

# %%
tipos_datos_Base_de_Datos = {
'FECHA_CORTE': 'datetime',
'Tipo_Entidad': 'uint16',
'Codigo_Entidad': 'uint16',
'Tipo_Patrimonio': 'uint16',
'Subtipo_Patrimonio': 'uint16',
'Estado': 'uint16',
'Cerrado': 'uint16',
'CODIGO_NEG': "uint32",
'TIPO_PARTICIPA': 'uint16'
}

tipos_datos_datos_abiertos = {
'fecha_corte': "datetime",
'tipo_entidad': "uint16",
'codigo_entidad': "uint16",
'tipo_negocio': "uint16",
'subtipo_negocio': "uint16",
'codigo_negocio': "uint32",
'tipo_participacion': "uint16"
}

df_Base_de_Datos = Types.convertir_tipo(df_Base_de_Datos,tipos_datos_Base_de_Datos)
df_Datos_Abiertos = Types.convertir_tipo(df_Datos_Abiertos,tipos_datos_datos_abiertos)


# %%
try:
    # Verifica si la columna existe antes de aplicar la función
    if "FECHA_CORTE" in df_Base_de_Datos.columns:
        df_Base_de_Datos['Fecha_num'] = df_Base_de_Datos["FECHA_CORTE"].apply(fecha_a_numero_serie)
    else:
        logging.warning('La columna "FECHA_CORTE" no existe en df_Base_de_Datos.')

    if "fecha_corte" in df_Datos_Abiertos.columns:
        df_Datos_Abiertos['Fecha_num'] = df_Datos_Abiertos["fecha_corte"].apply(fecha_a_numero_serie)
    else:
        logging.warning('La columna "fecha_corte" no existe en df_Datos_Abiertos.')

    # Ahora, intenta convertir a uint16
    df_Base_de_Datos['Fecha_num'] = df_Base_de_Datos['Fecha_num'].astype("uint16")
    df_Datos_Abiertos['Fecha_num'] = df_Datos_Abiertos['Fecha_num'].astype("uint16")

except (ValueError, TypeError) as e:
    logging.error(f'Error al convertir la columna a uint16: {e}')
except Exception as e:
    logging.error(f'Error inesperado: {e}')


try:
    # Verificar que las columnas necesarias existen en df_Base_de_Datos
    if "Fecha_num" in df_Base_de_Datos.columns and "CODIGO_NEG" in df_Base_de_Datos.columns:
        df_Base_de_Datos["Key"] = df_Base_de_Datos["Fecha_num"].astype(str) + df_Base_de_Datos["CODIGO_NEG"].astype(str)
    else:
        logging.warning('Columnas "Fecha_num" o "codigo_neg" no existen en df_Base_de_Datos.')

    # Verificar que las columnas necesarias existen en df_Datos_Abiertos
    if "Fecha_num" in df_Datos_Abiertos.columns and "codigo_negocio" in df_Datos_Abiertos.columns:
        df_Datos_Abiertos["Key"] = df_Datos_Abiertos["Fecha_num"].astype(str) + df_Datos_Abiertos["codigo_negocio"].astype(str)
    else:
        logging.warning('Columnas "Fecha_num" o "codigo_negocio" no existen en df_Datos_Abiertos.')

except (ValueError, TypeError) as e:
    logging.error(f'Error al crear la columna "Key": {e}')
except Exception as e:
    logging.error(f'Error inesperado: {e}')

# %%
Resumen_diferencias = pd.merge(
right=df_Base_de_Datos.groupby(["FECHA_CORTE","Key"]).agg({"Key":'count'}).rename(columns={"Key":"Cantidad"}).reset_index(),
left=df_Datos_Abiertos.groupby(["fecha_corte","Key"]).agg({"Key":'count'}).rename(columns={"Key":"Cantidad"}).reset_index(),
how='outer',
right_on=['FECHA_CORTE','Key'],
left_on=['fecha_corte','Key'],
suffixes=['_Datos_Abiertos','_Base_de_Datos']
)
Resumen_diferencias = Resumen_diferencias.fillna(0)

Resumen_diferencias['Diferencia'] = Resumen_diferencias['Cantidad_Base_de_Datos']-Resumen_diferencias['Cantidad_Datos_Abiertos']

Resumen_diferencias['Observacion'] = np.where(
    (Resumen_diferencias["Cantidad_Datos_Abiertos"]==0)&(Resumen_diferencias['Cantidad_Base_de_Datos']!=0),
    "Sin Registro en Datos Abiertos",
    np.where((Resumen_diferencias["Cantidad_Datos_Abiertos"]!=0)&(Resumen_diferencias['Cantidad_Base_de_Datos']==0),
    "Sin Registro en Base_de_Datos",
    np.where(
        Resumen_diferencias['Diferencia']==0,"No hay Diferencia","Diferencia en Registros"
    )
    )
)

tipo_dato = {
    'fecha_corte':'datetime',
    'FECHA_CORTE':'datetime'
}
Resumen_diferencias = Types.convertir_tipo(Resumen_diferencias,tipo_dato)

# %%
try:
    # Caso: 'Sin Registro en Datos Abiertos'
    if 'Sin Registro en Datos Abiertos' in Resumen_diferencias['Observacion'].unique():
        logging.warning('Se presentan registros en Base_de_Datos No reportados en Datos Abiertos')
        No_reportados_datos_abiertos = pd.merge(
            left=df_Base_de_Datos,
            right=Resumen_diferencias[Resumen_diferencias['Observacion'] == 'Sin Registro en Datos Abiertos'],
            how='inner',
            right_on=['Key', 'FECHA_CORTE'],
            left_on=['Key', 'FECHA_CORTE'],
            suffixes=['_Tera', '_agrupado']
        )[['FECHA_CORTE', 'Tipo_Entidad', 'Codigo_Entidad', 'Tipo_Patrimonio',
           'Nombre_Tipo_Patrimonio', 'Subtipo_Patrimonio', 'Estado', 'Cerrado',
           'Fecha_Hasta_Trans', 'CODIGO_NEG', 'TIPO_PARTICIPA', 'Fecha_num', 'Observacion']]

        logging.warning(f'La cantidad de registros no reportados en Datos Abiertos son {No_reportados_datos_abiertos.shape[0]}')
    else:
        logging.info('Todos los registros de Base_de_Datos se encuentran reportados en Datos Abiertos')
        No_reportados_datos_abiertos = pd.DataFrame()

    # Caso: 'Sin Registro en Base_de_Datos'
    if 'Sin Registro en Base_de_Datos' in Resumen_diferencias['Observacion'].unique():
        logging.warning('Se presentan registros en Datos Abiertos No reportados en Base_de_Datos')
        No_reportados_datos_Base_de_Datos = pd.merge(
            left=df_Datos_Abiertos,
            right=Resumen_diferencias[Resumen_diferencias['Observacion'] == 'Sin Registro en Base_de_Datos'],
            how='inner',
            right_on=['Key', 'fecha_corte'],
            left_on=['Key', 'fecha_corte'],
            suffixes=['_DA', '_agrupado']
        )[['fecha_corte', 'tipo_entidad', 'codigo_entidad', 'tipo_negocio',
           'subtipo_negocio', 'codigo_negocio', 'tipo_participacion', 'Fecha_num', 'Observacion']]

        logging.warning(f'La cantidad de registros no reportados en Base_de_Datos son {No_reportados_datos_Base_de_Datos.shape[0]}')
    else:
        logging.info('Todos los registros de Datos Abiertos se encuentran reportados en Base_de_Datos')
        No_reportados_datos_Base_de_Datos = pd.DataFrame()

    # Caso: 'Diferencia en Registros'
    if 'Diferencia en Registros' in Resumen_diferencias['Observacion'].unique():
        logging.warning('Se presentan diferencias entre la información reportada entre Base_de_Datos y Datos Abiertos')

        Registros_Diferencia_Datos_Abiertos = pd.merge(
            left=df_Datos_Abiertos,
            right=Resumen_diferencias[Resumen_diferencias['Observacion'] == 'Diferencia en Registros'],
            how='inner',
            right_on=['Key', 'fecha_corte'],
            left_on=['Key', 'fecha_corte'],
            suffixes=['_DA', '_agrupado']
        )[['fecha_corte', 'tipo_entidad', 'codigo_entidad', 'tipo_negocio',
           'subtipo_negocio', 'codigo_negocio', 'tipo_participacion', 'Fecha_num', 'Observacion']]

        Registros_Diferencia_Base_de_Datos = pd.merge(
            left=df_Base_de_Datos,
            right=Resumen_diferencias[Resumen_diferencias['Observacion'] == 'Diferencia en Registros'],
            how='inner',
            right_on=['Key', 'FECHA_CORTE'],
            left_on=['Key', 'FECHA_CORTE'],
            suffixes=['_Tera', '_agrupado']
        )[['FECHA_CORTE', 'Tipo_Entidad', 'Codigo_Entidad', 'Tipo_Patrimonio',
           'Nombre_Tipo_Patrimonio', 'Subtipo_Patrimonio', 'Estado', 'Cerrado',
           'Fecha_Hasta_Trans', 'CODIGO_NEG', 'TIPO_PARTICIPA', 'Fecha_num', 'Observacion']]

        logging.warning(f'Fechas con Diferencia Base_de_Datos: {Registros_Diferencia_Base_de_Datos["FECHA_CORTE"].unique()}')
        logging.warning(f'Fechas con Diferencia Datos Abiertos: {Registros_Diferencia_Datos_Abiertos["fecha_corte"].unique()}')
        logging.warning(f'Cantidad Registros con Diferencia Base_de_Datos: {Registros_Diferencia_Base_de_Datos.shape[0]}')
        logging.warning(f'Cantidad Registros con Diferencia Datos Abiertos: {Registros_Diferencia_Datos_Abiertos.shape[0]}')
    else:
        logging.info('No se presentan diferencias en los registros entre las dos tablas.')
        Registros_Diferencia_Datos_Abiertos = pd.DataFrame()
        Registros_Diferencia_Base_de_Datos = pd.DataFrame()
    
except (ValueError, TypeError) as e:
    logging.error(f'Error específico: {e}')
except Exception as e:
    logging.error(f'Error inesperado: {e}')


# %%
datos_negocios = []

for Negocio, group in df_Base_de_Datos.groupby('CODIGO_NEG'):
    
    fecha_min = group['FECHA_CORTE'].min()
    fecha_max = group['FECHA_CORTE'].max()
    
    rango_fechas_reportadas = pd.date_range(start=fecha_min, end=fecha_max, freq='D')

    fechas_reportadas = group['FECHA_CORTE'].unique()

    # Convertir fechas_reportadas a un DataFrame o Serie de pandas para poder usar set operations
    fechas_reportadas_set = set(fechas_reportadas)
    rango_fechas_set = set(rango_fechas_reportadas)

    # Encontrar fechas que están en el rango teórico pero no en las reportadas
    fechas_faltantes = rango_fechas_set - fechas_reportadas_set
    
    for fecha in fechas_faltantes:
        datos_negocios.append({
            'Codigo_Negocio': Negocio,
            'Fecha_Min': fecha_min,
            'Fecha_Max': fecha_max,
            'Cantidad_Rango_Teorico': len(rango_fechas_reportadas),
            'Cantidad_Rango_Reportado': len(fechas_reportadas),
            'Fecha_Faltante': fecha
        })
faltantes = pd.DataFrame(datos_negocios)

# %%
Nombres_Archivos = [
    {"Nombre Archivo": 'No_Registra_Datos_Abiertos.txt',
     "Ruta": os.path.join(Folder, 'No_Registra_Datos_Abiertos.txt')},
    {"Nombre Archivo": 'No_Registra_Base_de_Datos.txt',
     "Ruta": os.path.join(Folder, 'No_Registra_Base_de_Datos.txt')},
    {"Nombre Archivo": 'Detalle_Base_de_Datos_con_Diferencia.txt',
     "Ruta": os.path.join(Folder, 'Detalle_Base_de_Datos_con_Diferencia.txt')},
    {"Nombre Archivo": 'Detalle_Datos_Abiertos_con_diferencia.txt',
     "Ruta": os.path.join(Folder, 'Detalle_Datos_Abiertos_con_diferencia.txt')},
    {"Nombre Archivo": 'Detalle_Registros_Faltantes.txt',
     "Ruta": os.path.join(Folder, 'Detalle_Registros_Faltantes.txt')}
]

def crear_archivo_si_no_vacio(df, ruta):
    """Crea un archivo CSV si el DataFrame no está vacío y registra el proceso."""
    if not df.empty:
        try:
            df.to_csv(ruta, sep='|', index=False)
            logging.info(f'Se creó el archivo exitosamente en la ruta: {ruta}')
            return ruta  # Retorna la ruta para agregar a rutas_adjuntos
        except Exception as e:
            logging.error(f'Error al crear el archivo en la ruta {ruta}: {e}')
    return None


#parametro para solo filtar los registros faltantes a partir de dicha fecha
parametro = datetime.datetime(2022,1,1)

rutas_adjuntos = []

dataframes = [
    (No_reportados_datos_abiertos, Nombres_Archivos[0]['Ruta']),
    (No_reportados_datos_Base_de_Datos, Nombres_Archivos[1]['Ruta']),
    (Registros_Diferencia_Datos_Abiertos, Nombres_Archivos[2]['Ruta']),
    (Registros_Diferencia_Base_de_Datos, Nombres_Archivos[3]['Ruta']),
    (faltantes[faltantes['Fecha_Faltante']>=parametro], Nombres_Archivos[4]['Ruta'])
]

for df, ruta in dataframes:
    ruta_creada = crear_archivo_si_no_vacio(df, ruta)
    if ruta_creada:
        rutas_adjuntos.append(ruta_creada)
        #break  # Salir del bucle después de crear el primer archivo


# %%
rutas_adjuntos

# %%
def reemplazar_mensaje(mensaje, placeholder, valor, archivo_placeholder,archivo):
    """Reemplaza placeholders en el mensaje y registra el cambio."""
    if valor.empty:
        mensaje = mensaje.replace(placeholder, '0')
        mensaje = mensaje.replace(f'(Nombre del Archivo: <strong>{archivo_placeholder}</strong>)', '')
        logging.info(f'Se reemplazó {placeholder} con 0 y se eliminó el archivo {archivo_placeholder}.')
    else:
        cantidad = valor.shape[0]
        mensaje = mensaje.replace(placeholder, str(cantidad))
        mensaje = mensaje.replace(archivo_placeholder, archivo)
        logging.info(f'Se reemplazó {placeholder} con {cantidad} y se reemplazo {archivo_placeholder} con {archivo}')
    return mensaje

with open(Config.Mensaje_html_exito,encoding = 'utf-8') as Mensaje:
    mensaje_html_exito = Mensaje.read()

# Reemplazos de fechas
mensaje_html_exito = mensaje_html_exito.replace('_Fecha_', today.strftime('%Y-%m-%d'))
mensaje_html_exito = mensaje_html_exito.replace('_FechaInicio_', Fecha_inicio.strftime('%Y-%m-%d'))
mensaje_html_exito = mensaje_html_exito.replace('_FechaFin_', Fecha_Fin.strftime('%Y-%m-%d'))

# Reemplazos para cada DataFrame
mensaje_html_exito = reemplazar_mensaje(mensaje_html_exito, '_cantidad_No_Reportados_DA_', No_reportados_datos_abiertos, '_Archivo_1_',Nombres_Archivos[0]['Nombre Archivo'])
mensaje_html_exito = reemplazar_mensaje(mensaje_html_exito, '_cantidad_No_Reportada_TDA_', No_reportados_datos_Base_de_Datos, '_Archivo_2_',Nombres_Archivos[1]['Nombre Archivo'])
mensaje_html_exito = reemplazar_mensaje(mensaje_html_exito, '_Cantidad_Tera_', Registros_Diferencia_Datos_Abiertos, '_Archivo_3_',Nombres_Archivos[2]['Nombre Archivo'])
mensaje_html_exito = reemplazar_mensaje(mensaje_html_exito, '_Cantidad_Datos_abiertos_', Registros_Diferencia_Base_de_Datos, '_Archivo_4_',Nombres_Archivos[3]['Nombre Archivo'])
mensaje_html_exito = reemplazar_mensaje(mensaje_html_exito, '_sin_reporte_', faltantes, '_Archivo_5_',Nombres_Archivos[4]['Nombre Archivo'])

asunto = f"{today.strftime('%Y-%m-%d')} Validacion Exitosa Formato"

destinatarios = Config.config_odbc(Config.destinatarios)
Outlook.notificacion_outlook(
    destinatarios['destinatarios']['Formato'],
    destinatarios['Con_Copia']['Formato'],
    mensaje_html_exito,
    asunto,
    rutas_adjuntos
)
logging.shutdown()


