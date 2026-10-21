# Análisis Batch de Demanda Hospitalaria (Urgencias)

## Descripción de la Solución
Este proyecto implementa una solución de Big Data utilizando Apache Spark (PySpark) para analizar el volumen histórico de pacientes que ingresan a la unidad de urgencias de un hospital. El objetivo principal es clasificar y cuantificar la demanda de atención médica según el nivel de gravedad (Triage). 

El procesamiento en batch carga un conjunto de datos, realiza la limpieza de registros nulos y ejecuta un Análisis Exploratorio de Datos (EDA) para determinar la distribución de pacientes. Finalmente, el sistema genera una visualización gráfica de las tendencias históricas, lo cual es fundamental para optimizar la asignación de personal médico y camas en las distintas sedes.

## Tecnologías Utilizadas
* **Sistema Operativo:** Ubuntu Server
* **Framework de Procesamiento:** Apache Spark (PySpark)
* **Lenguaje:** Python 3
* **Librerías Adicionales:** Pandas, Matplotlib

## Archivos del Repositorio
* `batch_procesamiento.py`: Script principal que contiene la lógica de carga, transformación (EDA) y visualización usando Spark DataFrames.
* `historico_urgencias.csv`: Conjunto de datos de prueba con los registros de ingreso de pacientes.
* `README.md`: Documentación del proyecto.

## Instrucciones de Ejecución** 
   Asegúrese de tener instalado Apache Spark, Java (JDK 8 u 11) y Python 3 en su servidor Ubuntu. Además, instale las dependencias de visualización ejecutando:
   ```bash
   pip install pandas matplotlib
