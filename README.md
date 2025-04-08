# ASG
Automatización del procesamiento, limpieza y carga de datos climáticos (temperatura media mensual) provenientes del SMN-CONAGUA para análisis ASG en el sector financiero. El proyecto genera archivos estandarizados en Excel y los integra en una base de datos SQL Server para su uso en modelos de riesgo.


# ASG_TempMedia

Este repositorio automatiza el procesamiento de datos de **temperatura media mensual por región en México**, utilizados en modelos ASG (Ambiental, Social y de Gobernanza) para el análisis de riesgo climático en el sector financiero.

## Fuente de datos
Los archivos CSV se descargan del Servicio Meteorológico Nacional (CONAGUA):
[Resumen mensual de temperaturas y lluvias](https://smn.conagua.gob.mx/es/climatologia/temperaturas-y-lluvias/resumenes-mensuales-de-temperaturas-y-lluvias)

## Funciones del script

- Carga automática de archivos CSV.
- Homologación de estados y municipios.
- Limpieza y estandarización de coordenadas.
- Generación de columnas clave como `clave_int`, `CIUDAD`, `ESTADO_COMPLETO`.
- Exportación a Excel y carga a SQL Server.

## Estructura de salida

| FECHA      | LONGITUD | LATITUD | ESTADO_COMPLETO | clave_int | CIUDAD     | TempMedia |  
|------------|----------|---------|------------------|-----------|------------|------------|
| 2024-07-01 | -101.698 | 21.009  | Guanajuato       | SALMAGTO  | Salamanca  | 26.57      |

## Tecnologías

- Python
- Pandas
- SQLAlchemy
- Regex y limpieza de texto

## Autor

**Alberto Hernández**  



import os
import pandas as pd
from datetime import datetime
import unicodedata
from sqlalchemy import create_engine
import time
import re

# Iniciar el temporizador
start_time = time.time()

# Directorio que contiene los archivos CSV
folder_path = "./CSV/Media"  # Ruta generalizada para evitar exposición
new_folder_path = "./Excel"   # Ruta de salida generalizada

# Lista de estados válidos
estados_validos = [
    'AGS', 'BC', 'BCS', 'CAMP', 'CHIH', 'CHIS', 'COAH', 'DF', 'COL', 'DGO',
    'GRO', 'GTO', 'HGO', 'JAL', 'MEX', 'MICH', 'MOR', 'NAY', 'NL', 'OAX', 'PUE',
    'QRO', 'QROO', 'SIN', 'SLP', 'SON', 'TAB', 'TAMPS', 'TLAX', 'VER', 'YUC',
    'ZAC', 'CDMX'
]

homologacion_estado = {
    'TAMP': 'TAMPS', 'TAMS': 'TAMPS', 'DF': 'CDMX', 'HDO': 'HGO',
    'B.C.S.': 'BCS', 'B.C.': 'BC'
}

def normalize_string(s):
    if isinstance(s, str):
        s = s.lower()
        s = unicodedata.normalize('NFKD', s).encode('ascii', 'ignore').decode('utf-8')
        s = s.strip()
    else:
        s = ''
    return s

def split_municipio(municipio):
    parts = municipio.split(',', 1)
    if len(parts) == 1:
        parts = municipio.rsplit(' ', 1)
        if len(parts) == 1:
            parts.append('')
    return parts

def remove_common_part(nombre, estado):
    estado = normalize_string(estado)
    if estado and estado in nombre:
        return nombre.replace(estado, '').strip()
    return nombre

def mode_safe(series):
    mode_series = series.mode()
    return mode_series[0] if not mode_series.empty else ''

def limpiar_ciudad(texto):
    if not isinstance(texto, str):
        return ''
    texto = re.sub(r'[^a-zA-Z0-9\s]', '', texto)
    return texto.title()

def clean_string(s):
    if isinstance(s, str):
        return re.sub(r'[^A-Za-z]', '', s)[:5]
    return ''

# Procesamiento

dataframes = []

for filename in os.listdir(folder_path):
    if filename.endswith('.csv'):
        date_str = filename[:6]
        date = datetime.strptime(date_str, '%Y%m')
        csv_file_path = os.path.join(folder_path, filename)
        try:
            df = pd.read_csv(csv_file_path, encoding='utf-8')
        except UnicodeDecodeError:
            df = pd.read_csv(csv_file_path, encoding='ISO-8859-1')

        df = df.iloc[:, :6]
        df.columns = ['LONGITUD', 'LATITUD', 'ESTADO', 'CVE_SIH', 'MUNICIPIO', 'TempMedia']
        df['ESTADO'] = df['ESTADO'].replace(homologacion_estado)
        df[['CVE_SIH', 'ESTADO']] = df[['ESTADO', 'CVE_SIH']]
        df['MUNICIPIO'] = df['MUNICIPIO'].apply(normalize_string)
        df['MUNICIPIO_NOMBRE'], df['MUNICIPIO_ESTADO'] = zip(*df['MUNICIPIO'].apply(split_municipio))
        df['MUNICIPIO_ESTADO'] = df['MUNICIPIO_ESTADO'].str.strip().str.upper().replace(homologacion_estado)
        df['MUNICIPIO_NOMBRE'] = df.apply(lambda row: remove_common_part(row['MUNICIPIO_NOMBRE'], row['MUNICIPIO_ESTADO']), axis=1)
        df['FECHA'] = date
        df = df[['FECHA', 'LONGITUD', 'LATITUD', 'ESTADO', 'CVE_SIH', 'MUNICIPIO', 'MUNICIPIO_NOMBRE', 'MUNICIPIO_ESTADO', 'TempMedia']]
        df['ESTADO'] = df['ESTADO'].replace(homologacion_estado)
        dataframes.append(df)

temp_media_region = pd.concat(dataframes, ignore_index=True)

# Filtros y limpieza

temp_media_region = temp_media_region[
    pd.to_numeric(temp_media_region['LONGITUD'], errors='coerce').notnull() &
    pd.to_numeric(temp_media_region['LATITUD'], errors='coerce').notnull() &
    pd.to_numeric(temp_media_region['TempMedia'], errors='coerce').notnull()
]

temp_media_region['LONGITUD'] = pd.to_numeric(temp_media_region['LONGITUD'], errors='coerce')
temp_media_region['LATITUD'] = pd.to_numeric(temp_media_region['LATITUD'], errors='coerce')
temp_media_region['LONGITUD'] = temp_media_region.groupby('CVE_SIH')['LONGITUD'].transform(lambda x: round(x.mean(), 6))
temp_media_region['LATITUD'] = temp_media_region.groupby('CVE_SIH')['LATITUD'].transform(lambda x: round(x.mean(), 6))
temp_media_region['MUNICIPIO_NOMBRE'] = temp_media_region.groupby('CVE_SIH')['MUNICIPIO_NOMBRE'].transform(mode_safe)
temp_media_region['TempMedia'] = temp_media_region['TempMedia'].astype(float).round(6)
temp_media_region = temp_media_region.drop(columns=['MUNICIPIO', 'MUNICIPIO_ESTADO'])
temp_media_region = temp_media_region.rename(columns={'MUNICIPIO_NOMBRE': 'MUNICIPIO'})

estado_completo_dict = {
    "AGS": "Aguascalientes", "BC": "Baja California", "BCS": "Baja California Sur", "CAMP": "Campeche",
    "CHIH": "Chihuahua", "CHIS": "Chiapas", "COAH": "Coahuila", "DF": "Ciudad de México", "CDMX": "Ciudad de México",
    "COL": "Colima", "DGO": "Durango", "GRO": "Guerrero", "GTO": "Guanajuato", "HGO": "Hidalgo", "JAL": "Jalisco",
    "MEX": "Estado de México", "MICH": "Michoacán", "MOR": "Morelos", "NAY": "Nayarit", "NL": "Nuevo León",
    "OAX": "Oaxaca", "PUE": "Puebla", "QRO": "Querétaro", "QROO": "Quintana Roo", "SIN": "Sinaloa",
    "SLP": "San Luis Potosí", "SON": "Sonora", "TAB": "Tabasco", "TAMPS": "Tamaulipas", "TLAX": "Tlaxcala",
    "VER": "Veracruz", "YUC": "Yucatán", "ZAC": "Zacatecas"
}

temp_media_region['ESTADO_COMPLETO'] = temp_media_region['ESTADO'].map(estado_completo_dict)
temp_media_region['CIUDAD'] = temp_media_region['MUNICIPIO'].apply(limpiar_ciudad)
temp_media_region['clave_int'] = temp_media_region.apply(
    lambda row: (clean_string(row['MUNICIPIO']) + clean_string(row['ESTADO'])).upper(), axis=1
)

temp_media_region = temp_media_region.dropna(subset=['LONGITUD', 'LATITUD', 'TempMedia'])
fecha_count = temp_media_region.groupby('clave_int')['FECHA'].nunique()
valid_claves = fecha_count[fecha_count >= 11].index
temp_media_region = temp_media_region[temp_media_region['clave_int'].isin(valid_claves)]

def homogenize_lat_lon(group):
    lat_6_digits = group['LATITUD'].round(6)
    lon_6_digits = group['LONGITUD'].round(6)
    lat = lat_6_digits.mode().iloc[0] if not lat_6_digits.empty else group['LATITUD'].iloc[0]
    lon = lon_6_digits.mode().iloc[0] if not lon_6_digits.empty else group['LONGITUD'].iloc[0]
    group['LATITUD'] = lat
    group['LONGITUD'] = lon
    return group

# Homogenizar y filtrar

temp_media_region = temp_media_region.groupby('clave_int').apply(homogenize_lat_lon)
temp_media_region = temp_media_region.drop_duplicates(subset=['clave_int', 'FECHA'])
columns_to_sql = ['FECHA', 'LONGITUD', 'LATITUD', 'ESTADO_COMPLETO', 'clave_int', 'CIUDAD', 'TempMedia']
temp_media_region_sql = temp_media_region[columns_to_sql]

# Exportar archivo Excel
ultima_fecha = temp_media_region['FECHA'].max()
file_name = f"Temp_Media_{ultima_fecha.strftime('%B%Y')}.xlsx"
os.makedirs(new_folder_path, exist_ok=True)
excel_file_path = os.path.join(new_folder_path, file_name)
temp_media_region_sql.to_excel(excel_file_path, index=False)
print(f"Archivo guardado en: {excel_file_path}")

# Conexión a base de datos (ajustar con variables de entorno o configuración segura)
# engine = create_engine('mssql+pyodbc://user:password@host/db?driver=SQL+Server')
# temp_media_region_sql.to_sql('ASG_TempMedia', engine, if_exists='replace', index=False)

end_time = time.time()
print(f"Tiempo de ejecución: {end_time - start_time:.2f} segundos")

