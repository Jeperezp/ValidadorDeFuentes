import pandas as pd
import logging

def convertir_tipo(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    for columna, tipo in config.items():
        if columna in df.columns:
            try:
                logging.info(f'Convirtiendo columna "{columna}" al tipo "{tipo}"')
                
                if tipo == 'datetime':
                    df[columna] = pd.to_datetime(df[columna], errors='coerce')
                elif tipo == 'numeric':
                    df[columna] = pd.to_numeric(df[columna], errors='coerce')
                else:
                    df[columna] = df[columna].astype(tipo)
                
                logging.info(f'Columna "{columna}" convertida exitosamente')
            except Exception as e:
                logging.error(f'Error al convertir la columna "{columna}" al tipo "{tipo}": {str(e)}')
        else:
            logging.warning(f'La columna "{columna}" no existe en el DataFrame')
    
    return df
