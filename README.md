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
