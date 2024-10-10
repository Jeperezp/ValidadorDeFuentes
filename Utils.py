import requests
import pandas as pd
import logging
import pyodbc
from Config import login
import datetime
import holidays_co

today = datetime.date.today()
lista = []
for i in range((today.year)-1,(today.year)+1):
    lista.append(pd.DataFrame(data=holidays_co.get_colombia_holidays_by_year(i),
                           columns=["Fecha","Celebracion"]))

# Crea un DataFrame con los Feriados del Año
df_holidays = pd.concat(lista)

df_holidays["Fecha"] = pd.to_datetime(df_holidays["Fecha"])

def Connection_DatosAbiertos(start: str, end: str, url: str, date_field: str = 'fecha_corte'):
    """
    Establishing a connection to Datos Abiertos.

    Parameters:
    start (str): Start day of query 'yyyy-mm-dd'.
    end (str): End day of query 'yyyy-mm-dd'.
    url (str): URL of API.
    date_field (str): Name of the date field to filter on.

    Returns:
    DataFrame: DataFrame containing the fetched data.
    """

    def fetch_data(params):
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.warning(f"Error fetching data: {e}")
            return []

    all_data = []
    start = f"{start}T00:00:00"
    end = f"{end}T23:59:59"
    params = {
        "$limit": 1000000,
        "$offset": 0,
        "$where": f"{date_field} >= '{start}' and {date_field} <= '{end}'"
    }

    while True:
        data = fetch_data(params)
        if not data:
            break
        all_data.extend(data)
        params["$offset"] += len(data)

    return pd.DataFrame(all_data)


def conexion_bd(Query:str, Key:str) -> pd.DataFrame:
    """
    Connects to a database SOLIP using the provided configuration parameters and executes a SQL query.

    Parameters:
    Query (str): SQL query to execute on the database.
    Key (str):Key to identify ODBC configuration in JSON file
    Returns:
    pandas.DataFrame: DataFrame containing the results of the SQL query.

    Raises:
    Exception: If an error occurs during the connection or execution of the query.

    Example:
    >>> conexion_bd("SELECT * FROM example_table")
    """
    config = login[Key]
    try:
        # Establish a connection to the database using the provided DSN, username, and password from the configuration.
        conn = pyodbc.connect(f"DSN={config['DNS']};UID={config['Usuario']};PWD={config['Password']}")
        # Execute the SQL query and store the results in a DataFrame.
        df = pd.read_sql(Query, conn,)
        return df
    except Exception as e:
        # If an error occurs, print the error message.
        print(e)
    finally:
        # Close the connection to the database, regardless of whether there was an error or not.
        conn.close()

def pares(date_min: datetime.datetime, date_max: datetime.datetime):
    """
    Parameters:
    date_min (datetime): start day  
    date_max (datetime): end day 
    
    Returns:
    list: List of date pairs
    """
    fechas = []
    lista_par = []

    fecha_actual = date_min

    logging.info(f"Iniciando la generación de pares de fechas desde {date_min.strftime('%Y-%m-%d')} hasta {date_max.strftime('%Y-%m-%d')}.")

    while date_min <= date_max:
        fechas.append(date_min.strftime('%Y-%m-%d'))
        
        # Cambiar entre el 1 de enero y el 31 de diciembre
        if date_min.month == 1:
            date_min = date_min.replace(month=12, day=31, year=date_min.year)
            logging.info(f"Cambiando a {date_min.strftime('%Y-%m-%d')} (31 de diciembre).")
        else:
            date_min = date_min.replace(month=1, day=1, year=date_min.year + 1)
            logging.info(f"Cambiando a {date_min.strftime('%Y-%m-%d')} (1 de enero).")

    # Agregar la fecha de fin si es posterior a la última fecha generada
    if date_min > date_max:
        fechas.append(date_max.strftime('%Y-%m-%d'))
        logging.info(f"Agregando fecha final: {date_max.strftime('%Y-%m-%d')}.")

    # Obtener parejas ordenadas
    parejas = [(fechas[i], fechas[i + 1]) for i in range(0, len(fechas) - 1, 2)]

    logging.info(f"Se generaron {len(parejas)} parejas de fechas.")
    
    return parejas

#The code defines a function called `date_proc` that takes two parameters: `date_sys` and `days_`.
def date_proc(date_sys: datetime.date, days_: int) -> datetime.datetime:
    List_days = []
    list_weekend = []
    contador = 1

    logging.info(f"Iniciando el procesamiento de fechas desde {date_sys.strftime('%Y-%m-%d')} para obtener {days_} días hábiles.")

    while len(List_days) < days_:
        current_date = date_sys - datetime.timedelta(days=contador)

        if current_date.weekday() in {5, 6}:  # Sábado o Domingo
            list_weekend.append(current_date)
            logging.info(f"{current_date} es un fin de semana y se añade a la lista de fines de semana.")
        elif current_date in df_holidays["Fecha"].dt.date.values:  # Día feriado
            list_weekend.append(current_date)
            logging.info(f"{current_date} es un día feriado y se añade a la lista de fines de semana.")
        else:
            List_days.append(current_date)
            logging.info(f"{current_date} se añade a la lista de días hábiles.")

        contador += 1

    logging.info(f"Se encontraron {len(List_days)} días hábiles. El día más antiguo es {min(List_days)}.")
    return min(List_days)

#The code you provided defines a function called `dates` that takes in a parameter called `dates`.
def dates (dates):
    date_prod_f = date_proc(dates,2)
    date_prod_i = date_proc(dates,3)

    if (date_prod_f -date_prod_i).days >1:
        star = (date_prod_i +datetime.timedelta(days=1))
        end = date_prod_f
    else:
        star = date_prod_f
        end = date_prod_f

    star = datetime.date(star.year,star.month,star.day).strftime('%Y-%m-%d')
    end = datetime.date(end.year,end.month,end.day).strftime('%Y-%m-%d')
    return star, end

import calendar

def ultimo_dia (año, mes):
    _, ultimo_dia = calendar.monthrange(año,mes)
    return ultimo_dia

#la Lista Fechas tiene como objetivo almacenar las fechas de fin de mes desde la fecha de inicio hasta la fecha de corte
fechas = []

#se Crea un bucle el cual almacenara las fecha de fin de mes en la Lista fechas que se habia creado vacia inicialmente
def lista_pares(start,end):
    for j in range(start.year, end.year+1):
        for i in range(1, 13):
            dia = ultimo_dia(j,i)
            if datetime.date(j,i,ultimo_dia(j,i))<end:
                #print(datetime.date(j,i,dia))
                fechas.append([(datetime.date(j,i,1)).strftime('%Y-%m-%d'),(datetime.date(j,i,dia)).strftime('%Y-%m-%d')])
            else:
                pass
    fechas.append([datetime.date(end.year,end.month,1).strftime('%Y-%m-%d'),end.strftime('%Y-%m-%d')])
    return fechas

def verificar_fecha(today: datetime, holidays_df: pd.DataFrame) -> None:
    """
    Verifica si hoy es un fin de semana o un día feriado y registra la información.
    
    Args:
        today (datetime): Fecha actual.
        holidays_df (pd.DataFrame): DataFrame con feriados.
    """
    SABADO = 5
    DOMINGO = 6

    if today.weekday() in {SABADO, DOMINGO}:
        logging.info(f"{today}: El proceso no se ejecuta en fin de semana.")
        return False
    elif today in holidays_df["Fecha"].dt.date.values:
        logging.info(f"{today}: El proceso no se ejecuta en día feriado.")
        return False
    else:
        logging.info(f"{today}: Inicio ejecución del proceso.")
        return True
    
